package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/pin"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/upload"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/service"
)

func GetCoreTestOptions() []coreTesting.TestContextBuilderOption {
	return []coreTesting.TestContextBuilderOption{
		coreTesting.WithStatefulMockRenterService(),
		coreTesting.WithServiceFactory(core.UPLOAD_SERVICE, service.NewMetadataService),
		coreTesting.WithServiceFactory(core.PIN_SERVICE, service.NewPinService),
		coreTesting.WithServiceFactory(core.STORAGE_SERVICE, service.NewStorageService),
		coreTesting.WithServiceFactory(core.REQUEST_SERVICE, service.NewRequestService),
		coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
		coreTesting.WithServiceFactory(core.USER_SERVICE, service.NewUserService),
	}
}

func GetPluginTestOptions() []coreTesting.TestContextBuilderOption {
	return []coreTesting.TestContextBuilderOption{
		coreTesting.WithServiceFactory(pluginCore.PIN_SERVICE, pin.NewPinService),
		coreTesting.WithServiceFactory(pluginCore.UPLOAD_SERVICE, upload.NewUploadService),
		coreTesting.WithProtocol(internal.ProtocolName, protocol.NewProtocol),
		coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{}),
	}
}

func GetCommonTestOptions() []coreTesting.TestContextBuilderOption {
	return []coreTesting.TestContextBuilderOption{coreTesting.CombineOptions(GetCoreTestOptions(), GetPluginTestOptions())}
}

func GetDbTestOptions() []coreTesting.TestContextBuilderOption {
	return []coreTesting.TestContextBuilderOption{
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
	}
}

func GetTUSUploadTestOptions() []coreTesting.TestContextBuilderOption {
	return []coreTesting.TestContextBuilderOption{
		coreTesting.CombineOptions(GetCommonTestOptions(),
			coreTesting.WithServiceFactory(core.TUS_SERVICE, service.NewTUSService),
			coreTesting.WithAPI(internal.ProtocolName, api.NewAPI),
			coreTesting.WithAPIConfig(internal.ProtocolName, &pluginConfig.APIConfig{}),
			coreTesting.WithMockS3()),
	}
}

// HTTPTestClient wraps an HTTP client with helper methods for testing
type HTTPTestClient struct {
	client  *http.Client
	baseURL string
}

// NewHTTPTestClient creates a new HTTP test client with cookie jar support
func NewHTTPTestClient(baseURL string) (*HTTPTestClient, error) {
	jar := NewCookieJar()

	return &HTTPTestClient{
		client: &http.Client{
			Jar:     jar,
			Timeout: 30 * time.Second,
		},
		baseURL: baseURL,
	}, nil
}

// PostJSON makes a POST request with JSON data
func (c *HTTPTestClient) PostJSON(path string, data interface{}) (*http.Response, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	url := c.baseURL + path

	// Create request with proper headers
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	return c.client.Do(req)
}

// PostJSONWithAuth makes a POST request with JSON data and authentication
func (c *HTTPTestClient) PostJSONWithAuth(path string, data interface{}) (*http.Response, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	url := c.baseURL + path
	return c.client.Post(url, "application/json", bytes.NewBuffer(jsonData))
}

// Get makes a GET request
func (c *HTTPTestClient) Get(path string) (*http.Response, error) {
	url := c.baseURL + path
	return c.client.Get(url)
}

// RegisterUser registers a new user
func (c *HTTPTestClient) RegisterUser(email, password, firstName, lastName string) error {
	registerData := map[string]interface{}{
		"email":      email,
		"password":   password,
		"first_name": firstName,
		"last_name":  lastName,
	}

	resp, err := c.PostJSON("/api/auth/register", registerData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("registration failed with status: %d", resp.StatusCode)
	}

	return nil
}

// Login logs in a user
func (c *HTTPTestClient) Login(email, password string) error {
	loginData := map[string]interface{}{
		"email":    email,
		"password": password,
		"remember": true,
	}

	resp, err := c.PostJSON("/api/auth/login", loginData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("login failed with status: %d", resp.StatusCode)
	}

	return nil
}

// CreateAPIKey creates a new API key
func (c *HTTPTestClient) CreateAPIKey(name string) (string, error) {
	apiKeyData := map[string]interface{}{
		"name": name,
	}

	resp, err := c.PostJSONWithAuth("/api/account/keys", apiKeyData)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("API key creation failed with status: %d", resp.StatusCode)
	}

	var apiKeyResponse struct {
		Token string `json:"token"`
	}

	err = json.NewDecoder(resp.Body).Decode(&apiKeyResponse)
	if err != nil {
		return "", err
	}

	return apiKeyResponse.Token, nil
}
