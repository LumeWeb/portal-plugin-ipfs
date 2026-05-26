# @lumeweb/portal-plugin-ipfs

## 0.2.0 (2026-05-26)

### Breaking Changes

- Major changes

### Features

- add TUS default pre-finish response callback to emit CID upon upload completion
- update dependencies and add IPFS display name
- implement comprehensive file management and unpinning workflows
- integrate quota service for upload/download tracking
- enhance quota tracking with client IP and user ID
- Implement archive format handling and flexible upload processing
- add comprehensive observability with metrics and tracing
- add QUIC v1 and WebSocket announcement addresses
- integrate progress tracker into operation handlers
- add IPNS website hosting support
- refactor IPNS and website storage to use binary multihashes
- add SSL status tracking for website certificates
- add DNS hosting support for IPFS plugin
- add GetKeyByName method to IPNSKeyService
- integrate IPNS key auto-creation and publishing with websites
- add DNS zone and IPNS key foreign keys to websites
- add core DNS types and interfaces
- implement DNS service layer
- add DNS API endpoints and DTOs
- add database migrations for DNS zones
- add DNS zone management and validation
- make DHT implementation configurable with basic/fullrt modes
- implement hosting transition handlers with status resets
- add probabilistic peer attribution for bitswap downloads
- add optional reservation system support to download handlers
- implement quota reservations for upload operations
- add quota validation to upload processors and error handling
- add UnixFSType enum and separate block/unixfs sizes in BlockMetaResponse
- migrate avast/retry-go from v4 to v5 API
- expose gateway domain in website API responses and config endpoint
- inherit SSL state from soft-deleted website on recreate
- include nameservers in website config when DNS hosting is enabled
- support subdomain websites sharing a parent DNS zone
- add multi-transport support (WSS, QUIC, WebTransport)
- derive announce addresses from host listen addrs with AnnounceWeb DNS substitution
- add WebRTC-direct transport
- add ipns_key_id and active_cid to website response
- abstract DNS resolver and fix validation token expiry bugs
- return ValidateDNSResult from ValidateDNS, return 200 for expected outcomes
- add IPNS PubSub routing (DHT+GossipSub) with config-driven republish
- accept archive uploads, add upload result API
- auto-convert plain IPFS CID to IPNS target in CreateWebsite
- add IPFS delegated routing V1 server via boxo
- add PROXY protocol v1/v2 and bitswap want-block rate limiting

### Fixes

- need to ensure post files support seeking, else fall back to reading to memory
- need to ensure handling of datetime between mysql and sqlite
- add uniqueIndex to UnixFSNode BlockID
- update IPFS and libp2p dependencies to latest versions
- remove duplicate /api prefix from pin routes
- BeforeSave gets called before BeforeCreate, so we need to move the status empty check
- TUS route cannot be in a group as we can't have overlapping middleware
- prevent misleading "Deleted pin record" log when pin not found
- ensure workflow data is passed to next operation
- add missing protocol to workflow request
- remove mutex from concurrent unpin test to enable actual concurrency testing
- normalize empty parent_path to root path and validate file manager paths
- replace cid.Parse with cid.Cast and handle cast errors gracefully in convertFilePathToManagerItem
- validate file manager paths in listDirectoryContents endpoint
- handle root path case in GetBreadcrumbs
- add new file path workflow
- add storage hash to FilePath workflow in recomputePaths method
- add nil checks for upload service and metadata store in IPFS operations
- validate user ID before processing uploads
- unused import
- add default timestamps and engine specification to mysql migration schema
- goose statements are choking the parser/queries?
- trying to log the block error after having reset it, causes nil ptr
- add OPTIONS route for IPFS content addressing with dummy handler
- defensive size validation for TUS uploads
- improve download quota handling and error reporting
- handle missing client IP for quota tracking
- correct assertion order in IPFS get handler test
- validate download quota before fetching block data
- resolve race conditions and transaction context issues
- use mapstructures Unmarshaler properly
- extract manual progress tracker initialization to helper
- janitor job source ID and test fixtures
- remove invalid protocol name dependency from IPNS services
- prevent nil pointer panic in IPNS republisher
- prevent nil pointer panic in IPNS republisher
- prevent nil pointer panic in IPNS republisher
- separate SQL statements in MySQL migration
- use httputil.EncodeResponse for proper model-to-response conversion in SSL status handlers
- add NOT NULL constraint to ssl_status column in ipfs_websites table
- apply Kodus PR suggestions for DNS hosting implementation
- change INTEGER to BIGINT UNSIGNED for foreign key compatibility
- separate SQL statements into individual goose blocks
- register DNS service with correct ID and add as website dependency
- resolve schema cycle detection by using DTO types
- remove duplicate tags from SSL status webhook endpoint
- remove placeholder contact and terms of service
- use dto package for IPFS/IPNS path construction
- move foreign key constraints to dns_zones migration
- resolve Kodus suggestions for type safety and DNSLink path construction
- remove duplicate column additions in DNS zones migration
- resolve merge conflicts with develop
- add ipnsPublisherSvc field to WebsiteServiceDefault
- remove duplicate column additions in dns zones migration
- use service ID constants for IPNS publisher and republisher
- resolve service type mismatch for IPNS publisher
- resolve critical data loss bug in ImportZoneFile Replace mode
- resolve go.sum conflict with develop
- move go-zonefile to direct dependency and fix error codes
- remove dead code and fix API inconsistencies
- use directory name without file extension for LevelDB datastore
- remove redundant type assertions in protocol operations
- improve bash script error handling and remove duplicate code
- eliminate bash race conditions in ipfs command handling
- store target hash before deletion for IPNS republishing
- remove duplicate import and add nil check for DNS config
- resolve race conditions in bloom filter and map access
- address code issues identified by review
- distinguish between files and archives in format detection
- handle flaky reprovider mock expectations
- address PR review comments on race conditions and test bugs
- move pins endpoint to root level
- make /api/info endpoint publicly accessible
- update pin status to pinned after POST upload
- update pin status to pinned after TUS upload
- require pin service and fail fast when unavailable
- add user isolation to pins API and DB service
- standardize error handling in ForUser pin methods
- correct mock expectation ordering for ForUser methods
- return 404 instead of 500 when fetching replaced pins
- correctly propagate DB errors in GetPinByRequestIDForUser
- update workflow status assertion strings to match actual messages
- align IPNS API response codes with Swagger documentation
- remove type assertion from IPNS service methods
- extract CID from path instead of passing full path to PublishWithKey
- add LAN DHT protocol support and DRY up DHT configuration
- treat DNS addresses as public in allPeersLocal()
- add nil record checks to prevent nil pointer dereference
- add canonical domain handling and HTTP error checking for PowerDNS
- normalize nameserver format and DRY client implementation
- use Fatalf in powerdns test
- add concrete DTOs for list responses in swagger docs
- remove ID and timestamp fields from DNS record DTOs
- resolve goroutine race condition in Reprovider trigger mechanism
- return proper HTTP status codes for DNS records API
- address false positives and distinguish zone/record errors
- RRSet not found error by canonicalizing DNS names with trailing dots
- add WebsiteItemResponse for swagger generics
- properly handle dns_hosting_enabled field
- respect user's dns_hosting_enabled setting in website creation
- accept valid IPNS peer IDs from IPNS publish
- use proper test parameter in error assertions
- Fix IPNS validation to use peer.Decode
- Ensure only libp2p-key CIDs are accepted as IPNS targets
- auto-detect IPFS to IPNS conversion in UpdateWebsite
- remove duplicate GetCronJobIdentifier call from plugin cron job name
- set correct SourceID for WebsiteJanitorJob
- use WithExplicitJobType for website janitor
- extract CID from IPNS path before validation
- use type-safe GORM Model for IPNS key website reference check
- correct query order and type signatures in ListKeysWithFilters
- normalize CID for pin lookup and website creation
- normalize CID resolved from IPNS record
- handle unsupported CID versions and simplify path reconstruction
- reprovider test uses Maybe() for optional expectations
- add validation for dns_enabled field type
- use direct TXT query for DNS validation token
- use concrete swagger structs for list endpoints
- skip quota checks for internal name resolution operations
- add proper quota tracking for upload and storage operations
- use core.FireAsync instead of ctx.FireAsync
- use DetachContext to prevent canceled context contamination
- use DetachContext for quota event emission
- skip quota event when caller handles tracking
- check group quota availability for anonymous downloads
- gracefully handle missing quota service when checking group availability
- attribute downloads to upload records for proper quota tracking
- add missing context import to test files
- add mock upload service expectations to blockstore tests
- deregister inner bitswap receiver to enable wrapper's ReceiveMessage override
- fix context propagation and simplify tracker usage
- always emit download events when quotas enabled, even without IP address
- prevent nil pointer dereference in quota validation success logging
- use detached context for async quota events in upload service
- update quota API integration to match new interface
- address quota reservation and memory issues from PR review
- create per-block quota reservations for download tracking
- use core.Fire instead of core.FireAsync for quota emit helpers
- properly wrap quota errors with core error types in CheckWithReservation
- use UnixFSType.IsDirectory() to correctly detect HAMT shards as directories
- improve error handling, test utilities, and fix closure capture bug
- chmod go mod cache to fix fixture generation permission error
- use FindFixturesDir instead of GetDataDir for CAR file paths
- validate uploads using actual DAG block size
- resolve goroutine panic in reprovider tests
- resolve code quality issues
- store logical UnixFS file size in UnixFSNode.BlockSize
- resolve import cycle by moving GetStandardTestOptions to testopts package
- replace hardcoded UserID with dynamic user ID in TUS upload tests
- add option to disable pin timeout for large file uploads
- prevent infinite loop when workflow fails in pin wait
- enable reliable multi-block retrieval from external peers
- prevent race conditions and resource leaks in node operations
- optimize block processing parallelism and batch metadata writes
- flush metadata with detached context after block processing failure
- propagate flush error instead of silently swallowing it
- remove strict Times(2) mock expectation in TestReprovider_Run
- register ErrKeyFileUploadFailed in error registry
- remove unprocessed %s from ErrKeyFileUploadFailed message
- use retry-go for block processing to stop retrying on canceled context
- use core.GetServiceConfig instead of direct GetConfig for DNS config
- use pointer types in WithServiceConfig to match GetServiceConfig[*T]
- move GetStandardTestOptions to protocol/tests to avoid import cycle
- support partial updates for website update endpoint
- require target_type and target_hash together in partial updates
- make DNS zone creation idempotent and preserve zone ref on delete failure
- handle concurrent CreateZone race condition with duplicate key error recovery
- verify zone ownership in CreateZone when returning existing
- restore soft-deleted DNS zones on re-enable instead of failing on unique constraint
- recreate PowerDNS zone when restoring soft-deleted DNS zone
- PowerDNS TXT quoting, ALIAS apex records, and soft-deleted zone restore
- normalize trailing dots in DNS nameserver validation
- update DNS records when switching website from IPNS to IPFS target type
- also update DNS when switching IPFS to IPNS, not just IPNS to IPFS
- add dnslink= prefix to DNSLink TXT record values
- allow partial website updates and refactor IPNS key management
- add companion DHT for FullRT mode and aggregate close errors
- prevent nil pointer panic in IPNS republish by tracking last published CID
- update LastPublishedCID in DB using fetched record to satisfy BeforeSave
- pass configured announce addresses through AnnouncementAddresses and ConnectionAddresses
- auto-convert bare host:port announce addresses to multiaddr format
- nil-guard HTTPService in AddrsFactory and update unspecAddrs ports
- correct AnnounceWeb announcement addresses
- filter ephemeral UDP ports from announce addresses
- add plain TCP listener and announce address
- skip ephemeral port filter when configPort is 0
- nil-guard host addrs in debug logging
- allow private IPs when AnnounceWeb with domain
- filter non-configured TCP ports from announce addrs
- force public reachability for libp2p node
- disable TCP reuseport for Docker NAT compatibility
- exempt private IP ranges from rcmgr per-IP limits
- add fc00::/7 IPv6 ULA exemption to ConnRateLimiters
- set libp2p identify user agent to lumeweb-ipfs
- use CreateWebsiteDNSRecords for managed DNS token regeneration and add test
- merge both channels in SearchValue instead of returning one
- route IPNS PutValue through companion DHT instead of FullRT
- use detached context for IPNS publish and add boot-time republish
- StreamingBlockstore race condition and add upload result IDOR check
- populate parentBlock.ID after GORM upsert on MySQL
- DoneTracker double-unlock and flaky Reset race test
- make IPNS publish async in website CRUD and fix validation token overwrite bug
- add panic recovery to publishCIDAsync goroutine
- normalize CID to v1 at IPNS publish write boundary
- use normalized CID for IPNS path to match DB value
- resolve content resolution bottlenecks
- add WaitForPublishes to sync async publish goroutines in tests
- reject CIDv0 as valid IPNS target to prevent auto-conversion bypass
- accept raw peer IDs in routing IPNS endpoint
- use /128 for IPv6 gateway addresses in parseGatewayMultiaddrs
- use OS-assigned ports in tests and harden trusted proxy resolution

## 0.1.3

### Patch Changes

- 49e6d23: add build information support

## 0.1.2

### Patch Changes

- 21db2db: - Use lo ToPtr for pointer handling

## 0.1.1

### Patch Changes

- c020916: ---
  - Properly handle database prefixes with sqlite/mysql
  - Handle announcement address errors gracefully
  - Update CI to only trigger release on push events
  - Extend release workflow triggers and update concurrency settings
  - Update dependencies:
    - Upgrade mergo to 1.0.1
    - Upgrade go-multiaddr to 0.14.0
    - Upgrade portal to 0.2.1
    - Upgrade gox to 0.2.0
    - Update go.mod and go.sum

## 0.1.0

### Minor Changes

- 25ec910: Initial release
