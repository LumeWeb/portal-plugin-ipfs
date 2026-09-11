package coolify

import (
	"context"
)

// ResourceStatus is a coarse, provider-neutral status for a Coolify resource.
// Coolify exposes many finer statuses; the workspace service only needs the
// transitions it acts on.
type ResourceStatus string

const (
	ResourceStatusRunning  ResourceStatus = "running"
	ResourceStatusStarting ResourceStatus = "starting"
	ResourceStatusStopping ResourceStatus = "stopping"
	ResourceStatusExited   ResourceStatus = "exited"
	ResourceStatusReady    ResourceStatus = "ready"
	ResourceStatusFinished ResourceStatus = "finished"
	ResourceStatusQueued   ResourceStatus = "queued"
	ResourceStatusFailed   ResourceStatus = "failed"
	ResourceStatusUnknown  ResourceStatus = "unknown"

	// Health-aware application statuses. Coolify maps the container status to a
	// plain string (the generated Application.Status is a free-form string, no
	// enum is exposed), and when a Docker healthcheck is configured the running
	// container reports "healthy" or "unhealthy". "degraded" means the
	// container is up but in a failed/issuing state. These are used only by the
	// readiness decision; the workspace service never performs its own HTTP
	// probe against the application.
	ResourceStatusHealthy   ResourceStatus = "healthy"
	ResourceStatusUnhealthy ResourceStatus = "unhealthy"
	ResourceStatusDegraded  ResourceStatus = "degraded"
)

// CreatedResource is the result of a create operation: the provider resource
// UUID, recorded as soon as Coolify returns it.
type CreatedResource struct {
	UUID string
}

// CreateDatabaseRequest carries the placement and limit settings for a new
// database. Credentials are intentionally absent: Coolify generates them.
type CreateDatabaseRequest struct {
	ServerUUID        string
	ProjectUUID       string
	EnvironmentName   string
	EnvironmentUUID   string
	DestinationUUID   string
	Name              string
	Image             string
	MemoryLimit       string
	MemoryReservation string
	CPULimit          string
	Tags              []string
}

// CreateApplicationRequest carries the settings for a Docker-image
// application. Basic-auth credentials and the domain are set at creation.
//
// Tags lists the Coolify tags to assign to the application. Coolify v4.3.19
// persists application tags (HandlesTagsApi) and supports a server-side tag
// filter on GET /applications?tag=..., so tags ARE a supported
// correctness/recovery mechanism. The workspace service sends one
// installation-scoped tag (plus the deterministic name) so adoption after an
// ambiguous create timeout can be scoped to this installation.
type CreateApplicationRequest struct {
	ServerUUID         string
	ProjectUUID        string
	EnvironmentName    string
	EnvironmentUUID    string
	DestinationUUID    string
	Name               string
	Image              string
	Tag                string
	Port               string
	Domain             string
	BasicAuthUsername  string
	BasicAuthPassword  string
	HealthCheckEnabled bool
	// HealthCheckPath is the Docker/Coolify container health-check path, probed
	// by the container itself (localhost/container access), NOT a public Caddy
	// route. Coolify Basic Auth is deliberately not applied to it.
	HealthCheckPath       string
	HealthCheckPort       string
	HealthCheckMethod     string
	HealthCheckReturnCode int
	MemoryLimit           string
	MemoryReservation     string
	CPULimit              string
	NoindexDomains        []string
	Tags                  []string
}

// DatabaseResource holds the detail needed to wire the workspace runtime to
// its dedicated database. Credentials (Password, RootPassword) are transferred
// in-memory only and must never be logged, persisted, or returned from an API.
//
// Type is the Coolify database resource type ("mysql" or "mariadb") and is
// used to select the correct engine-specific credential set. RootPassword is
// the engine's root/admin password, used by the portal to provision logical
// databases/users on the shared resource; Username/Password/Database are the
// engine's generated application credentials.
type DatabaseResource struct {
	ID           string
	Status       ResourceStatus
	Type         string
	InternalURL  string
	Host         string
	Port         uint16
	Database     string
	Username     string
	Password     string
	RootPassword string
}

// ApplicationResource is the provider view of an application.
type ApplicationResource struct {
	ID     string
	Status ResourceStatus
	Domain string
}

// DeploymentResource tracks an application deployment by its deployment UUID.
type DeploymentResource struct {
	ID     string
	Status ResourceStatus
}

// StorageMount is a persistent volume attachment for an application.
type StorageMount struct {
	Name      string
	MountPath string
}

// EnvironmentVariable is a single runtime environment entry.
type EnvironmentVariable struct {
	Key    string
	Value  string
	Secret bool
}

// WorkspaceProvider is the provider-neutral boundary consumed by the workspace
// service. The Coolify adapter is the only implementation; tests supply fakes.
//
// Shared-resource lookup vs. logical provisioning: the workspace service never
// creates, starts, stops, or deletes a database resource per workspace. It owns
// ONE shared MySQL/MariaDB resource (WorkspaceConfig.Database.ResourceID) and
// uses GetDatabase to resolve that resource's current internal host/alias, then
// provisions a logical database/user on it through mysqlprovision.Engineer.
// Consequently the database create/start/stop/delete methods are NOT part of
// this interface. CreateDatabase remains available on the low-level Client for
// an optional one-time bootstrap outside workspace provisioning, but
// reconciliation never invokes it.
type WorkspaceProvider interface {
	// ResolveDatabaseResource resolves the shared MySQL/MariaDB resource's
	// current details (status, internal URL / host / port) by its Coolify
	// resource ID. It is used to discover the portal-side admin/connection
	// host for logical database provisioning and is never used to provision a
	// per-workspace resource.
	ResolveDatabaseResource(context.Context, string) (DatabaseResource, error)

	CreateApplication(context.Context, CreateApplicationRequest) (CreatedResource, error)
	GetApplication(context.Context, string) (ApplicationResource, error)
	SetApplicationEnvironment(context.Context, string, []EnvironmentVariable) error
	SetApplicationBasicAuth(context.Context, string, string, string) error
	EnsureApplicationStorage(context.Context, string, []StorageMount) error
	StartApplication(context.Context, string) (DeploymentResource, error)
	StopApplication(context.Context, string) error
	DeleteApplication(context.Context, string) error

	GetDeployment(context.Context, string) (DeploymentResource, error)
	// FindApplicationsByTag lists the applications carrying the given Coolify
	// tag via GET /applications?tag=... and returns them (without name
	// filtering). It is the tag-scoped recovery path: only applications this
	// installation tagged are candidates, so a same-named resource belonging to
	// another installation is never adopted. It must return every matching
	// application so the caller can fail closed on ambiguity. Coolify v4.3.19
	// persists tags and supports this filter.
	FindApplicationsByTag(context.Context, string) ([]Resource, error)
	// FindApplicationByName lists applications and returns those whose name
	// matches. It is the deterministic-name fallback used when no installation
	// tag is configured (and therefore a tagged lookup is unavailable). It must
	// return every matching application so the caller can fail closed on
	// ambiguity.
	FindApplicationByName(context.Context, string) ([]Resource, error)
}

// Resource is a minimal provider resource match used for recovery after an
// ambiguous create timeout. It carries enough to identify an application by
// its deterministic name.
type Resource struct {
	ID     string
	Type   string
	Name   string
	Status ResourceStatus
}

// Provider is the Coolify implementation of the provider-neutral
// WorkspaceProvider boundary. It adapts the lower-level Client (whose delete
// operations require explicit volume/configuration flags) to the coarse
// provider contract: deleting a workspace's resources always removes their
// volumes because the workspace runtime owns no data outside them.
type Provider struct {
	client *Client
}

// Ensure Provider satisfies the provider-neutral interface at compile time.
var _ WorkspaceProvider = (*Provider)(nil)

// NewProvider wraps a Coolify client in a Provider. The client owns the bearer
// token and base URL; callers must keep it alive for the Provider's lifetime.
func NewProvider(client *Client) *Provider {
	return &Provider{client: client}
}

// Client returns the underlying Coolify client. It is exposed for callers that
// need capabilities outside the provider-neutral boundary (e.g. health checks
// during startup validation).
func (p *Provider) Client() *Client { return p.client }

// ResolveDatabaseResource resolves the shared MySQL/MariaDB resource's current
// details by its Coolify resource ID. It never creates/stops/deletes the
// resource; it only looks it up so the portal can connect for logical
// database/user provisioning.
func (p *Provider) ResolveDatabaseResource(ctx context.Context, id string) (DatabaseResource, error) {
	return p.client.GetDatabase(ctx, id)
}

func (p *Provider) CreateApplication(ctx context.Context, req CreateApplicationRequest) (CreatedResource, error) {
	return p.client.CreateApplication(ctx, req)
}

func (p *Provider) GetApplication(ctx context.Context, id string) (ApplicationResource, error) {
	return p.client.GetApplication(ctx, id)
}

func (p *Provider) SetApplicationEnvironment(ctx context.Context, id string, env []EnvironmentVariable) error {
	return p.client.SetApplicationEnvironment(ctx, id, env)
}

func (p *Provider) SetApplicationBasicAuth(ctx context.Context, id string, username, password string) error {
	return p.client.SetApplicationBasicAuth(ctx, id, username, password)
}

func (p *Provider) EnsureApplicationStorage(ctx context.Context, id string, mounts []StorageMount) error {
	return p.client.EnsureApplicationStorage(ctx, id, mounts)
}

func (p *Provider) StartApplication(ctx context.Context, id string) (DeploymentResource, error) {
	return p.client.StartApplication(ctx, id)
}

func (p *Provider) StopApplication(ctx context.Context, id string) error {
	return p.client.StopApplication(ctx, id)
}

// DeleteApplication removes the application and its attached volumes. The
// provider contract offers no volume toggle; workspace application storage is
// owned entirely by the workspace, so it is always removed on deletion.
func (p *Provider) DeleteApplication(ctx context.Context, id string) error {
	return p.client.DeleteApplication(ctx, id, true, true)
}

func (p *Provider) GetDeployment(ctx context.Context, id string) (DeploymentResource, error) {
	return p.client.GetDeployment(ctx, id)
}

func (p *Provider) FindApplicationsByTag(ctx context.Context, tag string) ([]Resource, error) {
	return p.client.FindApplicationsByTag(ctx, tag)
}

func (p *Provider) FindApplicationByName(ctx context.Context, name string) ([]Resource, error) {
	return p.client.FindApplicationByName(ctx, name)
}
