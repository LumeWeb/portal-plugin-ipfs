# Platform-Provided Subdomains — Design Plan

**Status:** Draft (design consolidated from brainstorm session; simplified after pushback on over-engineering)
**Scope:** Introduce platform-owned subdomains that users can claim for their websites, alongside — not replacing — user-owned domains. Support alt roots (HNS et al.) and future purposes (staging, PR previews) without schema churn.

---

## 1. Goal

Let a user deploy a website on a **platform-provided subdomain** (`user-chosen-label.pinner.site`) they do not own, alongside or instead of a user-owned apex domain. The model must:

- Treat platform subdomains as **ordinary website domains** with one marker: "the parent zone is owned by the platform, not by the user."
- Support alt roots (Handshake, others) as first-class roots.
- Accommodate future purposes (staging sites, GitHub PR previews) without schema changes.
- Avoid special-cases sprinkled through the codebase — the model absorbs the concept once, cleanly.

Non-goals for v1:
- Paid-tier gating stored in the DB (upload quota is already the effective gate).
- A rename flow (delete + reclaim is acceptable).
- Suggestions in availability responses.

---

## 2. Background — Current Data Model

The existing model (already alt-root aware) is close to what we need:

- **`Website`** — owns the target (IPFS CID / IPNS key), `PrimaryDomainID` (FK), `IPNSKeyID`. Has no domain string.
- **`WebsiteDomain`** — polymorphic binding of a hostname to a website across namespaces (`icann`, `hns`). Carries delegation state (`Status`), per-domain DNS hosting (`DNSHostingEnabled`), a single canonical `ZoneID` (PowerDNS zone ref), and per-domain SSL state. Unique `(domain, namespace)`.
- **`DNSZone`** — one PowerDNS zone per (user, domain) pair. Row-level ownership by `UserID`.
- **`DelegatedDomainService`** — drives zone/record lifecycle via `DomainProvider` registry, applying the **one-zone rule**: apex owns zone, subdomain reuses parent's zone, and reuse across user IDs is rejected.
- **Providers** are namespace-keyed (`icann`, `hns`) and encapsulate validation, delegation record generation, apex record type, DANE/TLSA usage, DNSSEC requirements, and nameserver expectations.
- **Gateway** resolves sites via `GetWebsiteByDomain` → `website_domains` join → `Website`. It never needs to know who owns the subdomain's parent zone.

### One structural blocker

`DelegatedDomainService.resolveManagedZone` currently rejects parent-zone reuse across different users:

```go
if z.UserID != userID {
    return nil, false, fmt.Errorf("parent zone %q is owned by another user", parent)
}
```

That check is correct for user→user subdomain hierarchy. It becomes the single relaxable corner once we introduce explicitly registered **platform roots**.

---

## 3. Core Design — `PlatformDomain` (tiny) + one nullable FK on `WebsiteDomain`

### 3.1 New table: `platform_domains`

A minimal registry of platform-owned roots available for user subdomains. This table exists **only** to be the trust anchor for the one-zone relaxation — it answers the question "is this parent zone explicitly designated as platform-owned?"

```go
type PlatformDomain struct {
    ID        uint           `gorm:"primaryKey"`
    Domain    string         `gorm:"uniqueIndex"` // e.g. "pinner.site", "pinner"
    Namespace DomainNamespace                     // icann | hns
    ZoneID    uint           `gorm:"index"`       // the platform-owned DNSZone row
    Enabled   bool                                // can users claim new subdomains?
    CreatedAt time.Time
    UpdatedAt time.Time
}
```

That is the entire table.

**What this table does NOT have** (and why):
- No `LabelPolicy` — label rules can live at the API layer; the moment we need per-root variance it's config, not schema.
- No `ReservedNames` — same reasoning; rejection at the claim endpoint.
- No `Purpose` on the root — purposes live on the bindings (see §7); a root can host any mix until product says otherwise.
- No `OwnerUserID` — the platform zone's `DNSZone.UserID` *is* the operator user; we don't need a separate owner column.

### 3.2 One new column on `website_domains`

```go
// NULL — user-owned apex/normal binding (today's behavior).
// Set  — this binding is a platform subdomain minted under the referenced PlatformDomain.
PlatformDomainID *uint `gorm:"index"`
```

That is the entire change to the existing model.

**Why not store just the root string:** doing so requires splitting `Domain` on every read to discover "which root is this under?", and makes `platform_domains` useless as an FK target. The nullable FK keeps reads simple and keeps a referential link to the trust anchor.

**What this column does NOT carry:**
- No `Purpose` column on the binding — the future purposes (staging/PR preview) can be inferred from context or added later as a separate decision.
- No `IsPlatformSubdomain` bool — the FK presence answers that with strictly more information.

---

## 4. Lifecycle & Integrations

### 4.1 Claim flow (user-facing)

```
POST /websites/{websiteID}/domains
{ platform_domain: "pinner.site", label: "myblog" }        // explicit
{ platform_domain: "pinner.site", generate: true }          // computed
```

- Both variants call the existing `DelegatedDomainService.CreateDomain` with the composed domain (`myblog.pinner.site` or a generated one).
- Label rules are enforced at the API layer before calling into the service.
- For `generate: true`, the server runs a generate-and-retry loop: pick candidate (`adjective-noun-number`) → check availability → bind on success. Wordlists can be application-level static data until a root needs variance.
- On success, response is a normal `DomainResponse`.

### 4.2 Zone resolution — the one surgical change

`resolveManagedZone` gains a platform-root check before the user-parent-zone check:

```go
func (s *DelegatedDomainService) resolveManagedZone(ctx, domain, userID) (*DNSZone, bool, error) {
    parent := parentDomain(domain)
    namespace := namespaceFor(domain)

    // NEW: platform root lookup.
    if pd := lookupPlatformDomain(parent, namespace); pd != nil && pd.Enabled {
        z, _ := s.dnsSvc.GetZone(ctx, pd.ZoneID)
        return z, false, nil // never "created by us" — platform-owned
    }

    // Existing user-parent-zone logic unchanged.
    if parent != "" {
        z, err := s.dnsSvc.GetZoneByDomain(ctx, parent)
        ...
    }
    ...
}
```

The existing "parent zone owned by another user" hard error stays in place for user→user subdomains; it's bypassed only when the parent is an explicitly registered platform root.

**Trust boundary:** because only operators can register `PlatformDomain` rows (see §5), this relaxation is safe — every platform subdomain's zone provably descends from an operator-vouched root.

### 4.3 Verification flow

For platform subdomains the platform controls both sides of the DNS check, so user-side TXT verification is a no-op:

- When `wd.PlatformDomainID != nil`, skip the user-side TXT verification.
- `Status` transitions directly to `active` once records are written.

This is the only content special-case; one guard clause, clearly commented.

### 4.4 DNS, SSL, and DANE

All flows already work:

- **DNS hosting toggle** stays per-binding. For platform subdomains it's forced `true`/read-only at the API layer (you can't self-host DNS for a subdomain of someone else's zone) — one validation rule, not a schema change.
- **Per-domain SSL** is already on `WebsiteDomain` — each subdomain mints its own cert with no code changes.
- **DANE TLSA** (`SetTLSARecord` targeting `_443._tcp.<subdomain>`) already names records after the binding's domain rather than the zone apex — no changes.

### 4.5 Gateway resolution

`GetWebsiteByDomain` is unchanged. The gateway never sees `PlatformDomainID`. A `mysite.pinner.site` binding resolves identically to `mysite.com`.

### 4.6 Janitor

- `validateWebsite` is unchanged — platform-subdomain websites have targets, validate identically, get marked broken the same way.
- Future ephemeral purposes (PR previews) get a sweep keyed off metadata fields when that purpose is added — not now.

---

## 5. Governance — Who Can Create What

Platform domains are a **privileged, operator-governed registry**. The zone-reuse relaxation in §4.2 is only safe if every platform root is operator-vouched.

### 5.1 Operator side — admin CRUD

New routes in `AdminExtension` (admin role, alongside existing website block/unblock):

```
POST   /admin/api/ipfs/platform-domains           — register a new root
GET    /admin/api/ipfs/platform-domains           — list registered roots
PATCH  /admin/api/ipfs/platform-domains/:id       — toggle enabled
DELETE /admin/api/ipfs/platform-domains/:id       — deprecate (soft-delete)
```

**Registration is side-effect-full:** the operator must already have (or the call must create) the zone for this root, owned by an operator user. A `PlatformDomain` without a provisioned zone is invalid.

### 5.2 Bootstrap (initial roots)

For first-deploy: operators register initial roots through the admin API once the system is live. No config seeding — adding that later is a small additive feature.

### 5.3 User side — claiming subdomains

- Any authenticated user calls `POST /websites/{id}/domains` with `{platform_domain, label}` or `{platform_domain, generate: true}`.
- Authz check: the platform root must be `Enabled = true`.
- The DB enforces uniqueness via `(domain, namespace)`.

---

## 6. Availability & Naming DX

### 6.1 Availability check — **authenticated**

```
GET /platform-domains/availability?label=myblog
    →
{
  "label": "myblog",
  "results": [
    { "platform_domain": "pinner.site", "namespace": "icann", "available": true  },
    { "platform_domain": "pinner.dev",  "namespace": "icann", "available": false },
    { "platform_domain": "pinner",      "namespace": "hns",   "available": true  }
  ]
}

GET /platform-domains/{root}/availability?label=myblog
    →
{ "label": "myblog", "platform_domain": "pinner.site", "available": true }
```

**Properties:**
- Auth-required — attributable, rate-limited, auditable.
- Scoped to enabled platform roots — never probes user-managed zones.
- One row per root, binary answer per root.
- Labels pass through `NormalizeDomain` + `SanitizeDNSLabel` before lookup.

### 6.2 Generated names

When `generate: true`:
- Pattern: `adjective-noun-number` (`swift-river-42`).
- Wordlists live in code initially.
- Collision retry behind the unique constraint — server loops until an unclaimed pair lands.
- Non-sequential suffix.

Rename is **delete + reclaim** — separate rename endpoint deferred.

---

## 7. Future Purposes — Staging & PR Previews

These were the constraints that shaped the model; they're also how we know we didn't paint ourselves into a corner.

### 7.1 Staging sites

- URL shape: `staging.<user-site>.pinner.site` or `<user-site>.staging.pinner.dev`.
- Binding is just a normal platform subdomain with `PlatformDomainID` set — no special handling until product asks for it.

### 7.2 GitHub PR previews

PR numbers are repo-scoped, so `pr-123` alone collides across repos. The schema deliberately doesn't resolve this today:

**Open options (decided at product time):**
- **Site-scoped:** `pr-123.<website-slug>.pinner.site` (requires a stable website slug, orthogonal to this design).
- **Root-per-repo:** `<repo>.pinner.dev` as a separate `PlatformDomain`.
- **Opaque slugs:** `pr-x7k2pq.pinner.site`.

Whichever lands comes down to a row or two in `platform_domains`, not schema changes.

### 7.3 New purposes

Adding a new purpose = adding a row to `platform_domains` (if it's a new root) or just calling `createDomain` with the right `platform_domain` (if it's a new purpose on an existing root). No schema churn either way.

---

## 8. What We're Explicitly NOT Doing

- **Not** forking `website_domains` into a separate platform-subdomain table.
- **Not** a bool flag — nullable FK carries more information for the same cost.
- **Not** changing `DNSZone.UserID` semantics — platform zones are owned by an operator user row, same as any other zone.
- **Not** putting paid/free in the DB — upload quota already gates content creation.
- **Not** adding policy fields to the schema — label rules and reserved names live at the API layer until a real variance requirement shows up.
- **Not** unauthenticated availability checks — auth-only, binary, rate-limited.
- **Not** choosing a PR-preview naming convention now — the model supports every reasonable choice as data, not code.

---

## 9. Rollout Plan (high level)

1. **Schema migration** (`mysql` + `sqlite`):
   - `CREATE TABLE platform_domains` (per §3.1).
   - `ALTER TABLE website_domains ADD COLUMN platform_domain_id INTEGER NULL` + index.
2. **`PlatformDomain` model + admin CRUD** in `AdminExtension`.
3. **`resolveManagedZone` patch** (§4.2) + `VerifyDomain` skip guard (§4.3) + api-layer enforcement that platform subdomains force `dns_hosting_enabled=true`.
4. **User-facing domain create** — accept `{platform_domain, label}` / `{platform_domain, generate: true}` in the existing `createDomain` request shape.
5. **Availability endpoint** — auth-only, multi-root shape per §6.1.
6. **Register initial roots** via the admin API after deploy.
7. **Label generation helper** for `generate: true` flow.
8. **Optional followups when needed:** label rules moved from code to config/table only if they vary by root; PR-preview site slugs if we go the site-scoped route; preview bot user support in auth layer.

---

## 10. Open Questions

- Which platform roots launch at v1 (and in which namespaces)?
- Is `staging` a separate `Website` per environment, or a per-`WebsiteDomain` toggle on the same row? (Both work; product decision.)
- Do previews get a dedicated bot user row, or does CI reuse existing user API keys?
