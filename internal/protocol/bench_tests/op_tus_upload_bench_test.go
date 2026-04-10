package bench_tests

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"runtime/pprof"

	"github.com/docker/go-units"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	tusTestUtils "go.lumeweb.com/portal-plugin-ipfs/internal/testing/tus"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/testopts"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
)

func createLargeTestFile(tb coreTesting.TB, size int64) *os.File {
	tb.Helper()

	tempDir := tb.TempDir()
	filePath := filepath.Join(tempDir, fmt.Sprintf("test-file-%d.dat", size))

	file, err := os.Create(filePath)
	if err != nil {
		tb.Fatalf("Failed to create test file: %v", err)
	}

	chunkSize := 1024 * 1024
	chunk := make([]byte, chunkSize)
	bytesWritten := int64(0)

	for bytesWritten < size {
		remaining := size - bytesWritten
		if remaining < int64(chunkSize) {
			chunk = chunk[:remaining]
		}

		_, err := rand.Read(chunk)
		if err != nil {
			file.Close()
			tb.Fatalf("Failed to generate random data: %v", err)
		}

		_, err = file.Write(chunk)
		if err != nil {
			file.Close()
			tb.Fatalf("Failed to write to test file: %v", err)
		}

		bytesWritten += int64(len(chunk))
	}

	_, err = file.Seek(0, 0)
	if err != nil {
		file.Close()
		tb.Fatalf("Failed to seek to beginning of test file: %v", err)
	}

	return file
}

func benchmarkLargeUpload(tb coreTesting.TB, ctx coreTesting.TestContext, size int64) {
	testFile := createLargeTestFile(tb, size)
	defer testFile.Close()

	_, requestID, userID := tusTestUtils.SetupTUSUpload(tb, ctx, testFile, nil)

	wfTest := coreTesting.NewWorkflowTest(ctx)
	wf := wfTest.NewOperationWorkflow(core.TUSUploadOperationName(internal.ProtocolName))

	workflowOptions := []core.WorkflowOption{
		core.WithWorkflowUserID(userID),
		core.WithWorkflowSourceIP("127.0.0.1"),
	}

	req := wfTest.GetRequest(requestID)
	uploadStart := time.Now()

	wfTest.MustConvertRequestToWorkflow(requestID, wf, 0, workflowOptions...)
	wfTest.ExecuteWorkflowStep(req)
	wfTest.CompleteWorkflowStep(req)

	uploadDuration := time.Since(uploadStart)
	tb.Logf("Upload: %v, Size: %.2f GB", uploadDuration, float64(size)/(1024*1024*1024))

	tusTestUtils.AssertTUSWorkflowSuccess(wfTest, req)

	pinSvc := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
	if pinSvc == nil {
		tb.Skip("Pin service not available")
	}

	startTime := time.Now()
	maxPinTimeout := 30 * time.Minute
	if timeoutStr := os.Getenv("TUS_BENCH_PIN_TIMEOUT"); timeoutStr != "" {
		if duration, err := time.ParseDuration(timeoutStr); err == nil {
			maxPinTimeout = duration
		}
	}

	for time.Since(startTime) < maxPinTimeout {
		sort := []filter.Sort{{Field: "created_at", Order: filter.OrderDesc}}
		pins, _, _ := pinSvc.ListPins(ctx, nil, sort, queryutil.DefaultPagination)

		for _, pin := range pins {
			if pin.UserID == userID && pin.Status == db.PinningStatusPinned {
				pinDuration := time.Since(startTime)
				throughputMBps := float64(size) / (1024 * 1024) / uploadDuration.Seconds()
				tb.Logf("Pin wait: %v, Throughput: %.2f MB/s", pinDuration, throughputMBps)
				return
			}
		}
		time.Sleep(5 * time.Second)
	}
	tb.Fatalf("Timeout waiting for pin status")
}

type benchmarkTestCase struct {
	name string
	size int64
}

var benchmarkCases = []benchmarkTestCase{
	{"1GB", 1 * units.GB},
	{"10GB", 10 * units.GB},
	{"100GB", 100 * units.GB},
	{"1TB", units.TB},
}

func BenchmarkTUSUpload(b *testing.B) {
	for _, tc := range benchmarkCases {
		b.Run(tc.name, func(b *testing.B) {
			if testing.Short() && tc.name == "1TB" {
				b.Skip("Skipping 1TB benchmark in short mode")
			}
			b.ResetTimer()
			for b.Loop() {
				coreTesting.RunTestCaseWithDB(b, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
					benchmarkLargeUpload(tb, ctx, tc.size)
				}, testopts.GetStandardTestOptions()...)
			}
		})
	}
}

func BenchmarkTUSUpload_Profile(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping performance profiling in short mode")
	}
	if os.Getenv("TUS_BENCH_ENABLE_PROFILING") != "true" {
		b.Skip("Set TUS_BENCH_ENABLE_PROFILING=true to enable profiling")
	}

	profilesDir := os.Getenv("TUS_BENCH_PROFILES_DIR")
	if profilesDir == "" {
		profilesDir = b.TempDir()
	}

	cpuProfileFile := filepath.Join(profilesDir, "cpu.prof")
	memProfileFile := filepath.Join(profilesDir, "mem.prof")

	f, err := os.Create(cpuProfileFile)
	if err != nil {
		b.Fatalf("Could not create CPU profile: %v", err)
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		b.Fatalf("Could not start CPU profile: %v", err)
	}
	defer pprof.StopCPUProfile()

	for _, tc := range benchmarkCases {
		b.Run(tc.name+"-Profile", func(b *testing.B) {
			b.ResetTimer()
			for b.Loop() {
				coreTesting.RunTestCaseWithDB(b, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
					benchmarkLargeUpload(tb, ctx, tc.size)
				}, testopts.GetStandardTestOptions()...)
			}
		})
	}

	f2, err := os.Create(memProfileFile)
	if err != nil {
		b.Fatalf("Could not create memory profile: %v", err)
	}
	defer f2.Close()
	runtime.GC()
	if err := pprof.WriteHeapProfile(f2); err != nil {
		b.Fatalf("Could not write memory profile: %v", err)
	}

	b.Logf("Profiles saved to %s", profilesDir)
}
