package engine

import (
	"context"
	"errors"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRunSpeedTestPreservesUIContract(t *testing.T) {
	backend := &fakeSpeedBackend{}
	server := &fakeSpeedServer{
		backend:      backend,
		targetURL:    "https://speed.example/speedtest/upload.php",
		displayName:  "Berlin, Germany",
		downloadMbps: 200,
		uploadMbps:   40,
		latency:      12_500 * time.Microsecond,
	}
	backend.server = server

	progress := testProgress()
	runSpeedTest(context.Background(), progress, 10*time.Second, 0, backend)

	result := <-progress.Result
	if result.DownloadMbps != 200 {
		t.Fatalf("DownloadMbps = %v, want 200", result.DownloadMbps)
	}
	if result.UploadMbps != 40 {
		t.Fatalf("UploadMbps = %v, want 40", result.UploadMbps)
	}
	if result.PingMs != 12.5 {
		t.Fatalf("PingMs = %v, want 12.5", result.PingMs)
	}
	if result.DownloadPeak != 200 {
		t.Fatalf("DownloadPeak = %v, want final speed floor of 200", result.DownloadPeak)
	}
	if result.UploadPeak != 40 {
		t.Fatalf("UploadPeak = %v, want final speed floor of 40", result.UploadPeak)
	}
	if progress.ServerName != "Berlin, Germany" {
		t.Fatalf("ServerName = %q", progress.ServerName)
	}
	if len(progress.URLs) != 1 || progress.URLs[0] != server.targetURL {
		t.Fatalf("URLs = %#v", progress.URLs)
	}
	if progress.Err != nil {
		t.Fatalf("unexpected error: %v", progress.Err)
	}

	wantPhases := []Phase{
		PhaseFinding,
		PhaseConnected,
		PhaseDownload,
		PhaseUpload,
		PhaseLatency,
		PhaseDone,
	}
	for i, want := range wantPhases {
		select {
		case got := <-progress.Phases:
			if got != want {
				t.Fatalf("phase %d = %v, want %v", i, got, want)
			}
		default:
			t.Fatalf("phase %d (%v) was not emitted", i, want)
		}
	}

	downloadSample := <-progress.Samples
	uploadSample := <-progress.Samples
	if downloadSample.Phase != PhaseDownload || downloadSample.Rate != 25_000_000 {
		t.Fatalf("download sample = %#v", downloadSample)
	}
	if uploadSample.Phase != PhaseUpload || uploadSample.Rate != 5_000_000 {
		t.Fatalf("upload sample = %#v", uploadSample)
	}
}

func TestRunSpeedTestReportsDiscoveryErrorAndStillFinishes(t *testing.T) {
	progress := testProgress()
	backend := &fakeSpeedBackend{findErr: errors.New("blocked")}

	runSpeedTest(context.Background(), progress, time.Second, 0, backend)

	if progress.Err == nil || !strings.Contains(progress.Err.Error(), "could not find") {
		t.Fatalf("progress.Err = %v", progress.Err)
	}
	result := <-progress.Result
	if result != (Result{}) {
		t.Fatalf("result = %#v, want zero-value partial result", result)
	}
	if got := <-progress.Phases; got != PhaseFinding {
		t.Fatalf("first phase = %v", got)
	}
	if got := <-progress.Phases; got != PhaseDone {
		t.Fatalf("last phase = %v", got)
	}
}

func TestResultMbpsFallsBackToTransferredBytes(t *testing.T) {
	got := resultMbps(0, 25_000_000, 10*time.Second, 10*time.Second)
	if got != 20 {
		t.Fatalf("resultMbps() = %v, want 20", got)
	}
}

func TestBytesPerSecToMbps(t *testing.T) {
	got := BytesPerSecToMbps(125_000)
	if math.Abs(got-1) > 1e-9 {
		t.Fatalf("BytesPerSecToMbps() = %v, want 1", got)
	}
}

func TestServerDisplayName(t *testing.T) {
	tests := []struct {
		name    string
		country string
		want    string
	}{
		{name: "Berlin", country: "Germany", want: "Berlin, Germany"},
		{name: "Berlin", want: "Berlin"},
		{country: "Germany", want: "Germany"},
	}
	for _, test := range tests {
		if got := serverDisplayName(test.name, test.country); got != test.want {
			t.Fatalf("serverDisplayName(%q, %q) = %q, want %q", test.name, test.country, got, test.want)
		}
	}
}

func TestLiveSpeedtestNet(t *testing.T) {
	if os.Getenv("RIPTIDE_LIVE_TEST") != "1" {
		t.Skip("set RIPTIDE_LIVE_TEST=1 to use real Speedtest.net traffic")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	progress := testProgress()

	Run(ctx, progress, 2, 2*time.Second)
	result := <-progress.Result

	if progress.Err != nil {
		t.Fatalf("live speed test failed: %v", progress.Err)
	}
	if result.DownloadMbps <= 0 {
		t.Fatalf("DownloadMbps = %v, want a positive live result", result.DownloadMbps)
	}
	if result.UploadMbps <= 0 {
		t.Fatalf("UploadMbps = %v, want a positive live result", result.UploadMbps)
	}
	if result.PingMs <= 0 {
		t.Fatalf("PingMs = %v, want a positive live result", result.PingMs)
	}
	t.Logf(
		"server=%q download=%.2f Mbps upload=%.2f Mbps ping=%.2f ms",
		progress.ServerName,
		result.DownloadMbps,
		result.UploadMbps,
		result.PingMs,
	)
}

func testProgress() *Progress {
	return &Progress{
		Phases:  make(chan Phase, 16),
		Samples: make(chan Sample, 16),
		Result:  make(chan Result, 1),
	}
}

type fakeSpeedBackend struct {
	server           speedServer
	findErr          error
	downloadCallback func(float64, uint64)
	uploadCallback   func(float64, uint64)
	totalDownload    uint64
	totalUpload      uint64
}

func (b *fakeSpeedBackend) FindServer(context.Context) (speedServer, error) {
	return b.server, b.findErr
}

func (b *fakeSpeedBackend) SetDownloadCallback(callback func(float64, uint64)) {
	b.downloadCallback = callback
}

func (b *fakeSpeedBackend) SetUploadCallback(callback func(float64, uint64)) {
	b.uploadCallback = callback
}

func (b *fakeSpeedBackend) TotalDownload() uint64 {
	return b.totalDownload
}

func (b *fakeSpeedBackend) TotalUpload() uint64 {
	return b.totalUpload
}

type fakeSpeedServer struct {
	backend      *fakeSpeedBackend
	targetURL    string
	displayName  string
	downloadMbps float64
	uploadMbps   float64
	latency      time.Duration
}

func (s *fakeSpeedServer) TargetURL() string {
	return s.targetURL
}

func (s *fakeSpeedServer) DisplayName() string {
	return s.displayName
}

func (s *fakeSpeedServer) Download(context.Context) error {
	s.backend.totalDownload = 250_000_000
	s.backend.downloadCallback(25_000_000, s.backend.totalDownload)
	return nil
}

func (s *fakeSpeedServer) Upload(context.Context) error {
	s.backend.totalUpload = 50_000_000
	s.backend.uploadCallback(5_000_000, s.backend.totalUpload)
	return nil
}

func (s *fakeSpeedServer) Ping(context.Context) (time.Duration, error) {
	return s.latency, nil
}

func (s *fakeSpeedServer) DownloadMbps() float64 {
	return s.downloadMbps
}

func (s *fakeSpeedServer) UploadMbps() float64 {
	return s.uploadMbps
}
