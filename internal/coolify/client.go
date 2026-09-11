package coolify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"go.lumeweb.com/portal-plugin-ipfs/internal/coolify/api"
)

// Client wraps the generated Coolify API client with:
//   - bearer token request editing;
//   - base URL normalization;
//   - response status validation;
//   - conversion of generated response wrappers into provider types.
//
// Secret-safe logging is a responsibility of the caller; this type never
// formats response bodies into errors.
type Client struct {
	generated *api.ClientWithResponses
	baseURL   string
	token     string
}

// NewClient creates a Coolify client wrapper. The baseURL may or may not
// include a trailing slash; it is normalized here.
func NewClient(baseURL, token string) (*Client, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("coolify: base url is required")
	}
	if token == "" {
		return nil, fmt.Errorf("coolify: api token is required")
	}
	normalized := strings.TrimRight(baseURL, "/")
	generated, err := api.NewClientWithResponses(normalized, api.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		return nil
	}))
	if err != nil {
		return nil, fmt.Errorf("coolify: init client: %w", err)
	}
	return &Client{generated: generated, baseURL: normalized, token: token}, nil
}

// BaseURL returns the normalized base URL.
func (c *Client) BaseURL() string { return c.baseURL }

// Host parses the host portion of the base URL for comparison during adoption.
func (c *Client) Host() string {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return ""
	}
	return u.Host
}

// Health checks the Coolify health endpoint.
func (c *Client) Health(ctx context.Context) error {
	resp, err := c.generated.HealthcheckWithResponse(ctx)
	if err != nil {
		return fmt.Errorf("coolify: health: %w", err)
	}
	return StatusCodeError(resp.HTTPResponse, resp.Body)
}

// CreateDatabase creates a MariaDB database resource and returns its UUID.
func (c *Client) CreateDatabase(ctx context.Context, req CreateDatabaseRequest) (CreatedResource, error) {
	var out CreatedResource
	body := api.CreateDatabaseMariadbJSONBody{
		ServerUuid:              req.ServerUUID,
		ProjectUuid:             req.ProjectUUID,
		EnvironmentName:         req.EnvironmentName,
		EnvironmentUuid:         req.EnvironmentUUID,
		DestinationUuid:         strPtr(req.DestinationUUID),
		Name:                    strPtr(req.Name),
		Image:                   strPtr(req.Image),
		IsPublic:                boolPtr(false),
		InstantDeploy:           boolPtr(false),
		LimitsMemory:            strPtr(req.MemoryLimit),
		LimitsMemoryReservation: strPtr(req.MemoryReservation),
		LimitsCpus:              strPtr(req.CPULimit),
		Tags:                    &req.Tags,
	}
	resp, err := c.generated.CreateDatabaseMariadbWithResponse(ctx, api.CreateDatabaseMariadbJSONRequestBody(body))
	if err != nil {
		return out, fmt.Errorf("coolify: create database: %w", err)
	}
	if cerr := StatusCodeError(resp.HTTPResponse, resp.Body); cerr != nil {
		return out, cerr
	}
	if resp.JSON200 == nil {
		return out, fmt.Errorf("coolify: create database response missing resource id")
	}
	out.UUID = resp.JSON200.Uuid
	return out, nil
}

// GetDatabase returns database details. Sensitive fields are pointers and are
// expected to be non-nil only when the token has read:sensitive permission;
// when they are absent they are left empty and the caller fails closed. The
// engine-specific credential set is selected by the resource Type so both
// MySQL and MariaDB shared resources are supported.
func (c *Client) GetDatabase(ctx context.Context, resourceID string) (DatabaseResource, error) {
	var out DatabaseResource
	resp, err := c.generated.GetDatabaseByUuidWithResponse(ctx, resourceID)
	if err != nil {
		return out, fmt.Errorf("coolify: get database: %w", err)
	}
	if cerr := StatusCodeError(resp.HTTPResponse, resp.Body); cerr != nil {
		return out, cerr
	}
	if resp.JSON200 == nil {
		return out, fmt.Errorf("coolify: get database response missing body")
	}
	d := resp.JSON200
	out.ID = d.Uuid
	out.Status = ResourceStatus(d.Status)
	out.Type = d.Type
	out.InternalURL = deref(d.InternalDbUrl)
	switch {
	case strings.EqualFold(d.Type, "mariadb"):
		out.Username = deref(d.MariadbUser)
		out.Password = deref(d.MariadbPassword)
		out.Database = deref(d.MariadbDatabase)
		out.RootPassword = deref(d.MariadbRootPassword)
	case strings.EqualFold(d.Type, "mysql"):
		out.Username = deref(d.MysqlUser)
		out.Password = deref(d.MysqlPassword)
		out.Database = deref(d.MysqlDatabase)
		out.RootPassword = deref(d.MysqlRootPassword)
	default:
		// Unknown/unsupported engine type: leave credentials empty so the
		// caller fails closed on the missing sensitive/resource fields.
	}
	return out, nil
}

// StartDatabase queues a database start.
func (c *Client) StartDatabase(ctx context.Context, resourceID string) error {
	resp, err := c.generated.StartDatabaseByUuidWithResponse(ctx, resourceID)
	if err != nil {
		return fmt.Errorf("coolify: start database: %w", err)
	}
	return StatusCodeError(resp.HTTPResponse, resp.Body)
}

// StopDatabase queues a database stop.
func (c *Client) StopDatabase(ctx context.Context, resourceID string) error {
	resp, err := c.generated.StopDatabaseByUuidWithResponse(ctx, resourceID, &api.StopDatabaseByUuidParams{})
	if err != nil {
		return fmt.Errorf("coolify: stop database: %w", err)
	}
	return StatusCodeError(resp.HTTPResponse, resp.Body)
}

// DeleteDatabase deletes a database resource and its volumes.
func (c *Client) DeleteDatabase(ctx context.Context, resourceID string, deleteVolumes bool, deleteConfigurations bool) error {
	resp, err := c.generated.DeleteDatabaseByUuidWithResponse(ctx, resourceID, &api.DeleteDatabaseByUuidParams{
		DeleteVolumes:        boolPtr(deleteVolumes),
		DeleteConfigurations: boolPtr(deleteConfigurations),
	})
	if err != nil {
		return fmt.Errorf("coolify: delete database: %w", err)
	}
	return StatusCodeError(resp.HTTPResponse, resp.Body)
}

// CreateApplication creates a Docker-image application and returns its UUID.
func (c *Client) CreateApplication(ctx context.Context, req CreateApplicationRequest) (CreatedResource, error) {
	var out CreatedResource
	healthCheckReturnCode := req.HealthCheckReturnCode
	body := api.CreateDockerimageApplicationJSONBody{
		ProjectUuid:             req.ProjectUUID,
		ServerUuid:              req.ServerUUID,
		EnvironmentName:         req.EnvironmentName,
		EnvironmentUuid:         req.EnvironmentUUID,
		DestinationUuid:         strPtr(req.DestinationUUID),
		Name:                    strPtr(req.Name),
		DockerRegistryImageName: req.Image,
		DockerRegistryImageTag:  strPtr(req.Tag),
		PortsExposes:            strPtr(req.Port),
		Domains:                 strPtr(req.Domain),
		InstantDeploy:           boolPtr(false),
		IsForceHttpsEnabled:     boolPtr(true),
		AutogenerateDomain:      boolPtr(false),
		IsHttpBasicAuthEnabled:  boolPtr(true),
		HttpBasicAuthUsername:   strPtr(req.BasicAuthUsername),
		HttpBasicAuthPassword:   strPtr(req.BasicAuthPassword),
		HealthCheckEnabled:      boolPtr(req.HealthCheckEnabled),
		HealthCheckPath:         strPtr(req.HealthCheckPath),
		HealthCheckPort:         strPtr(req.HealthCheckPort),
		HealthCheckMethod:       strPtr(req.HealthCheckMethod),
		HealthCheckReturnCode:   &healthCheckReturnCode,
		LimitsMemory:            strPtr(req.MemoryLimit),
		LimitsMemoryReservation: strPtr(req.MemoryReservation),
		LimitsCpus:              strPtr(req.CPULimit),
		NoindexDomains:          &req.NoindexDomains,
		Tags:                    strSlicePtr(req.Tags),
	}
	resp, err := c.generated.CreateDockerimageApplicationWithResponse(ctx, api.CreateDockerimageApplicationJSONRequestBody(body))
	if err != nil {
		return out, fmt.Errorf("coolify: create application: %w", err)
	}
	if cerr := StatusCodeError(resp.HTTPResponse, resp.Body); cerr != nil {
		return out, cerr
	}
	if resp.JSON201 == nil {
		return out, fmt.Errorf("coolify: create application response missing resource id")
	}
	out.UUID = resp.JSON201.Uuid
	return out, nil
}

// GetApplication returns application details.
func (c *Client) GetApplication(ctx context.Context, resourceID string) (ApplicationResource, error) {
	var out ApplicationResource
	resp, err := c.generated.GetApplicationByUuidWithResponse(ctx, resourceID)
	if err != nil {
		return out, fmt.Errorf("coolify: get application: %w", err)
	}
	if cerr := StatusCodeError(resp.HTTPResponse, resp.Body); cerr != nil {
		return out, cerr
	}
	if resp.JSON200 == nil {
		return out, fmt.Errorf("coolify: get application response missing body")
	}
	a := resp.JSON200
	out.ID = deref(a.Uuid)
	out.Status = ResourceStatus(deref(a.Status))
	out.Domain = deref(a.Fqdn)
	return out, nil
}

// SetApplicationEnvironment upserts the application environment in bulk.
func (c *Client) SetApplicationEnvironment(ctx context.Context, resourceID string, envs []EnvironmentVariable) error {
	type item struct {
		Key         string `json:"key"`
		Value       string `json:"value"`
		IsShownOnce *bool  `json:"is_shown_once,omitempty"`
		IsLiteral   *bool  `json:"is_literal,omitempty"`
	}
	data := make([]item, 0, len(envs))
	for _, e := range envs {
		v := item{Key: e.Key, Value: e.Value}
		if e.Secret {
			v.IsShownOnce = boolPtr(true)
			v.IsLiteral = boolPtr(true)
		}
		data = append(data, v)
	}
	payload := struct {
		Data []item `json:"data"`
	}{Data: data}
	buf, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("coolify: marshal env: %w", err)
	}
	resp, err := c.generated.UpdateEnvsByApplicationUuidWithBodyWithResponse(ctx, resourceID, "application/json", bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("coolify: update application env: %w", err)
	}
	return StatusCodeError(resp.HTTPResponse, resp.Body)
}

// SetApplicationBasicAuth updates an application's HTTP Basic Auth
// credentials by patching it. This is used to rotate the proxy Basic Auth
// credential so the new value takes effect on the deployed application.
//
// The generated UpdateApplicationByUuid JSON body type does not expose the
// basic-auth fields (the upstream OpenAPI only lists them on the create
// endpoints), so we send a minimal JSON payload through the raw-body variant.
// Coolify's update endpoint accepts the basic-auth fields on a PATCH even
// though they are absent from the schema.
func (c *Client) SetApplicationBasicAuth(ctx context.Context, resourceID string, username, password string) error {
	payload := struct {
		IsHTTPBasicAuthEnabled *bool   `json:"is_http_basic_auth_enabled"`
		HTTPBasicAuthUsername  *string `json:"http_basic_auth_username"`
		HTTPBasicAuthPassword  *string `json:"http_basic_auth_password"`
	}{
		IsHTTPBasicAuthEnabled: boolPtr(true),
		HTTPBasicAuthUsername:  strPtr(username),
		HTTPBasicAuthPassword:  strPtr(password),
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("coolify: marshal basic auth: %w", err)
	}
	resp, err := c.generated.UpdateApplicationByUuidWithBody(ctx, resourceID, "application/json", bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("coolify: update application basic auth: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("coolify: read update application basic auth response: %w", err)
	}
	return StatusCodeError(resp, body)
}

// GetApplicationStorage lists persistent and file storage for an application.
func (c *Client) GetApplicationStorage(ctx context.Context, resourceID string) ([]StorageMount, error) {
	var out []StorageMount
	resp, err := c.generated.ListStoragesByApplicationUuidWithResponse(ctx, resourceID)
	if err != nil {
		return out, fmt.Errorf("coolify: list application storage: %w", err)
	}
	if cerr := StatusCodeError(resp.HTTPResponse, resp.Body); cerr != nil {
		return out, cerr
	}
	if resp.JSON200 == nil || resp.JSON200.PersistentStorages == nil {
		return out, nil
	}
	for _, s := range *resp.JSON200.PersistentStorages {
		out = append(out, StorageMount{
			Name:      s.Name,
			MountPath: s.MountPath,
		})
	}
	return out, nil
}

// EnsureApplicationStorage creates any configured storage mounts that are
// missing, keyed by mount path.
func (c *Client) EnsureApplicationStorage(ctx context.Context, resourceID string, mounts []StorageMount) error {
	existing, err := c.GetApplicationStorage(ctx, resourceID)
	if err != nil {
		return err
	}
	byPath := make(map[string]bool, len(existing))
	for _, e := range existing {
		byPath[e.MountPath] = true
	}
	for _, m := range mounts {
		if byPath[m.MountPath] {
			continue
		}
		body := api.CreateStorageByApplicationUuidJSONBody{
			Type:      "persistent",
			Name:      strPtr(m.Name),
			MountPath: m.MountPath,
		}
		resp, err := c.generated.CreateStorageByApplicationUuidWithResponse(ctx, resourceID, api.CreateStorageByApplicationUuidJSONRequestBody(body))
		if err != nil {
			return fmt.Errorf("coolify: create application storage: %w", err)
		}
		if cerr := StatusCodeError(resp.HTTPResponse, resp.Body); cerr != nil {
			return cerr
		}
		byPath[m.MountPath] = true
	}
	return nil
}

// StartApplication starts an application and returns the deployment UUID.
func (c *Client) StartApplication(ctx context.Context, resourceID string) (DeploymentResource, error) {
	var out DeploymentResource
	resp, err := c.generated.StartApplicationByUuidWithResponse(ctx, resourceID, &api.StartApplicationByUuidParams{
		InstantDeploy: boolPtr(false),
	})
	if err != nil {
		return out, fmt.Errorf("coolify: start application: %w", err)
	}
	if cerr := StatusCodeError(resp.HTTPResponse, resp.Body); cerr != nil {
		return out, cerr
	}
	if resp.JSON200 != nil {
		out.ID = deref(resp.JSON200.DeploymentUuid)
		out.Status = ResourceStatus("queued")
	}
	return out, nil
}

// StopApplication stops an application.
func (c *Client) StopApplication(ctx context.Context, resourceID string) error {
	resp, err := c.generated.StopApplicationByUuidWithResponse(ctx, resourceID, &api.StopApplicationByUuidParams{
		DockerCleanup: boolPtr(true),
	})
	if err != nil {
		return fmt.Errorf("coolify: stop application: %w", err)
	}
	return StatusCodeError(resp.HTTPResponse, resp.Body)
}

// DeleteApplication deletes an application resource.
func (c *Client) DeleteApplication(ctx context.Context, resourceID string, deleteVolumes bool, deleteConfigurations bool) error {
	resp, err := c.generated.DeleteApplicationByUuidWithResponse(ctx, resourceID, &api.DeleteApplicationByUuidParams{
		DeleteVolumes:        boolPtr(deleteVolumes),
		DeleteConfigurations: boolPtr(deleteConfigurations),
	})
	if err != nil {
		return fmt.Errorf("coolify: delete application: %w", err)
	}
	return StatusCodeError(resp.HTTPResponse, resp.Body)
}

// GetDeployment returns deployment status by deployment UUID.
func (c *Client) GetDeployment(ctx context.Context, deploymentID string) (DeploymentResource, error) {
	var out DeploymentResource
	resp, err := c.generated.GetDeploymentByUuidWithResponse(ctx, deploymentID)
	if err != nil {
		return out, fmt.Errorf("coolify: get deployment: %w", err)
	}
	if cerr := StatusCodeError(resp.HTTPResponse, resp.Body); cerr != nil {
		return out, cerr
	}
	if resp.JSON200 != nil {
		out.ID = deploymentID
		out.Status = ResourceStatus(deref(resp.JSON200.Status))
	}
	return out, nil
}

// FindApplicationsByTag lists the applications carrying the given Coolify tag
// via GET /applications?tag=... and returns them (without name filtering).
// Coolify v4.3.19 persists application tags and supports this server-side tag
// filter, so the caller can adopt/recover an application scoped to its own
// installation tag. It returns every match so the caller can fail closed on
// ambiguity.
func (c *Client) FindApplicationsByTag(ctx context.Context, tag string) ([]Resource, error) {
	var out []Resource
	if tag == "" {
		return out, errors.New("coolify: tag query requires a non-empty tag")
	}
	resp, err := c.generated.ListApplicationsWithResponse(ctx, &api.ListApplicationsParams{Tag: &tag})
	if err != nil {
		return out, fmt.Errorf("coolify: list applications by tag: %w", err)
	}
	if cerr := StatusCodeError(resp.HTTPResponse, resp.Body); cerr != nil {
		return out, cerr
	}
	if resp.JSON200 == nil {
		return out, nil
	}
	for _, a := range *resp.JSON200 {
		out = append(out, Resource{
			ID:     deref(a.Uuid),
			Type:   "application",
			Name:   deref(a.Name),
			Status: ResourceStatus(deref(a.Status)),
		})
	}
	return out, nil
}

// FindApplicationByName lists the applications in the placement and returns
// those matching the given deterministic name. It is the deterministic-name
// fallback recovery/adoption path used when no installation tag is configured
// (and therefore a tagged lookup is unavailable), after a create timeout that
// may have persisted the resource before the response was lost (e.g. a network
// timeout). Coolify's GET /applications endpoint has no name filter, so we
// list all applications and filter client-side by name, returning every match
// so the caller can fail closed on ambiguity.
func (c *Client) FindApplicationByName(ctx context.Context, name string) ([]Resource, error) {
	var out []Resource
	resp, err := c.generated.ListApplicationsWithResponse(ctx, &api.ListApplicationsParams{})
	if err != nil {
		return out, fmt.Errorf("coolify: list applications: %w", err)
	}
	if cerr := StatusCodeError(resp.HTTPResponse, resp.Body); cerr != nil {
		return out, cerr
	}
	if resp.JSON200 == nil {
		return out, nil
	}
	for _, a := range *resp.JSON200 {
		if deref(a.Name) != name {
			continue
		}
		out = append(out, Resource{
			ID:     deref(a.Uuid),
			Type:   "application",
			Name:   deref(a.Name),
			Status: ResourceStatus(deref(a.Status)),
		})
	}
	return out, nil
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func boolPtr(b bool) *bool { return &b }

// strSlicePtr returns a pointer to a copy of ss when it is non-empty, so the
// JSON body only carries tags when there are any to send.
func strSlicePtr(ss []string) *[]string {
	if len(ss) == 0 {
		return nil
	}
	return &ss
}
