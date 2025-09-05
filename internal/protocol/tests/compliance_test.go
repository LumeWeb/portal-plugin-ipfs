package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "go.lumeweb.com/portal-plugin-core"
	dashboard "go.lumeweb.com/portal-plugin-dashboard"
	_ "go.lumeweb.com/portal-plugin-ipfs"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/service"
)

var plugins = core.GetPlugins()
var errorNS = core.ExportAllErrorNamespaces()

type ComplianceReport struct {
	Total   int  `json:"total"`
	Passed  int  `json:"passed"`
	Failed  int  `json:"failed"`
	Success bool `json:"success"`
}

func TestIPFSPinningServiceCompliance(t *testing.T) {
	coreTesting.RunTestCaseWithComponents(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		httpServ := core.GetService[core.HTTPService](ctx, core.HTTP_SERVICE)
		// Get base URL from the test context's HTTP service
		acctURL := fmt.Sprintf("%s:%d", httpServ.APISubdomain("dashboard", true), ctx.Config().Config().Core.Port)
		ipfsURL := fmt.Sprintf("%s:%d", httpServ.APISubdomain(internal.ProtocolName, true), ctx.Config().Config().Core.Port)

		// Create HTTP test client
		client, err := NewHTTPTestClient(acctURL)
		require.NoError(tb, err)

		// Register a new user with a unique email for each test run using plus addressing
		email := fmt.Sprintf("portal-ci-test+%d@lumeweb.com", time.Now().UnixNano())
		password := "testpassword123"

		err = client.RegisterUser(email, password, "Compliance", "Test")
		require.NoError(tb, err)

		// Login
		err = client.Login(email, password)
		require.NoError(tb, err)

		// Create API key
		authToken, err := client.CreateAPIKey("compliance-test-key")
		require.NoError(tb, err)

		pinningServiceEndpoint := ipfsURL

		// Use npx as the command with a context timeout
		cmdParts := []string{"npx", "-y", "@ipfs-shipyard/pinning-service-compliance", "-s", pinningServiceEndpoint, authToken}

		// Set working directory to the test file's directory
		_, currentFile, _, _ := runtime.Caller(0)
		workDir := filepath.Dir(currentFile)

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		// Handle NODE_PATH environment variable
		var extraEnv []string
		if nodePath := os.Getenv("NODE_PATH"); nodePath != "" {
			// Get the directory containing the node executable
			nodeDir := filepath.Dir(nodePath)

			// Derive npx path from node directory
			npxPath := filepath.Join(nodeDir, "npx")

			// Update cmdParts with npx path
			cmdParts[0] = npxPath

			// Append node directory to PATH
			currentPath := os.Getenv("PATH")
			newPath := nodeDir + string(filepath.ListSeparator) + currentPath
			extraEnv = append(os.Environ(), "PATH="+newPath)
		}

		// Create the command with context
		cmd := exec.CommandContext(ctx, cmdParts[0], cmdParts[1:]...)
		cmd.Dir = workDir

		// Apply extra environment if set
		if extraEnv != nil {
			cmd.Env = extraEnv
		}

		// Capture output
		var out bytes.Buffer
		var stderr bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr

		// Run the command
		err = cmd.Run()
		if err != nil {
			// Check if the error was due to context timeout
			if ctx.Err() == context.DeadlineExceeded {
				tb.Fatalf("Compliance test timed out after 5 minutes. Stderr: %s", stderr.String())
			}
		}

		// Log output for debugging if debug flag is set
		if os.Getenv("COMPLIANCE_DEBUG") != "" {
			if out.Len() > 0 {
				tb.Logf("Compliance test stdout: %s", out.String())
			}
			if stderr.Len() > 0 {
				tb.Logf("Compliance test stderr: %s", stderr.String())
			}
		}

		// Parse the output to extract the report file path
		output := out.String()

		// Find the report file path in the output using case insensitive regex
		var reportPath string
		re := regexp.MustCompile(`(?i)See the full report at\s+(.+?\.md)`)
		matches := re.FindStringSubmatch(output)
		if len(matches) > 1 {
			reportPath = matches[1]
			// Replace .md with .json
			reportPath = strings.TrimSuffix(reportPath, ".md") + ".json"
		}

		// Assert that the command ran successfully
		require.NoError(tb, err, "Compliance test failed to run. Please check if npx is installed and @ipfs-shipyard/pinning-service-compliance is available")

		// If we found a report path, read and parse it, then assert the results
		if reportPath != "" {
			reportData, readErr := os.ReadFile(reportPath)
			require.NoError(tb, readErr, "Failed to read compliance report")

			var report ComplianceReport
			jsonErr := json.Unmarshal(reportData, &report)
			require.NoError(tb, jsonErr, "Failed to parse compliance report JSON")

			// Log the results
			tb.Logf("Compliance test results: Total=%d, Passed=%d, Failed=%d", report.Total, report.Passed, report.Failed)

			// Assert that all tests passed
			require.True(tb, report.Success, "Compliance test suite did not succeed")
			require.Equal(tb, report.Total, report.Passed, "Not all compliance tests passed")
			require.Equal(tb, 0, report.Failed, "Some compliance tests failed")
		} else {
			// If no report path was found, fail the test
			tb.Fatal("Could not find compliance report path in output")
		}
	},
		coreTesting.TestComponents(coreTesting.ComponentHTTP, coreTesting.ComponentCron, coreTesting.ComponentDB),
		coreTesting.CombineOptions(
			GetCoreTestOptions(),
			coreTesting.WithMockS3(),
			coreTesting.WithHTTPService(),
			coreTesting.WithServiceFactory(core.AUTH_SERVICE, service.NewAuthService),
			coreTesting.WithEnvConfigOrDefault("core.domain", "", "localhost"),
			coreTesting.WithPlugins(plugins...),
			coreTesting.WithConfig("plugin.dashboard.api.subdomain", "account"),
			coreTesting.WithAPIConfig("dashboard", &dashboard.APIConfig{
				Subdomain: "account",
			}),
			coreTesting.WithConfig("core.secure", false),
			coreTesting.WithErrorNamespaces(errorNS),
		),
	)
}
