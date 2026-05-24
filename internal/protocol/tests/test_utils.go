package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.lumeweb.com/portal-plugin-ipfs/internal/plugin"
	coreTesting "go.lumeweb.com/portal/core/testing"
	serviceTesting "go.lumeweb.com/portal/service/testing"
)

func GetStandardTestOptions() []coreTesting.TestContextBuilderOption {
	return []coreTesting.TestContextBuilderOption{
		serviceTesting.PresetE2E(),
		coreTesting.WithConfig("core.mail.host", "localhost"),
		coreTesting.WithConfig("core.mail.port", 25),
		coreTesting.WithConfig("plugin.ipfs.protocol.port", 0),
		coreTesting.WithConfig("plugin.ipfs.protocol.ws_port", 0),
		coreTesting.WithPlugins(plugin.GetPluginInfoWithTemplates(nil)),
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
