package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultConnections is the number of parallel streams used by the
	// Speedtest.net backend to saturate the link.
	DefaultConnections = 5
	// DefaultDuration is how long each transfer phase (download / upload) runs.
	DefaultDuration = 10 * time.Second

	sampleInterval = 100 * time.Millisecond
	connectedPause = 900 * time.Millisecond
	latencyTimeout = 8 * time.Second
)

// Sample is one instantaneous speed measurement broadcast to the UI each tick.
type Sample struct {
	Phase Phase   // which phase produced this sample
	Bytes uint64  // bytes transferred so far in the current phase
	Rate  float64 // instantaneous rate in bytes/sec
	At    time.Time
}

// Progress is the live channel payload the UI consumes.
type Progress struct {
	URLs       []string   // discovered target URLs (set once)
	ServerName string     // human-readable server/region label (set once)
	Phases     chan Phase // phase transitions
	Samples    chan Sample
	Result     chan Result
	Err        error
}

// Phase enumerates the test lifecycle.
type Phase int

const (
	PhaseInit Phase = iota
	PhaseFinding
	PhaseConnected
	PhaseDownload
	PhaseUpload
	PhaseLatency
	PhaseDone
)

func (p Phase) String() string {
	switch p {
	case PhaseFinding:
		return "finding servers"
	case PhaseConnected:
		return "connected"
	case PhaseDownload:
		return "download"
	case PhaseUpload:
		return "upload"
	case PhaseLatency:
		return "measuring latency"
	case PhaseDone:
		return "done"
	default:
		return "starting"
	}
}

// Result is the final measurement summary.
type Result struct {
	DownloadMbps float64
	UploadMbps   float64
	PingMs       float64
	DownloadPeak float64
	UploadPeak   float64
	Client       string // human-readable location/ISP if known
}

// speedBackend isolates the TUI-facing engine from the concrete provider.
// The production implementation uses Speedtest.net through speedtest-go;
// tests use a deterministic in-memory backend.
type speedBackend interface {
	FindServer(context.Context) (speedServer, error)
	SetDownloadCallback(func(rateBytesPerSec float64, totalBytes uint64))
	SetUploadCallback(func(rateBytesPerSec float64, totalBytes uint64))
	TotalDownload() uint64
	TotalUpload() uint64
}

type speedServer interface {
	TargetURL() string
	DisplayName() string
	Download(context.Context) error
	Upload(context.Context) error
	Ping(context.Context) (time.Duration, error)
	DownloadMbps() float64
	UploadMbps() float64
}

// Run executes the full test: discover a Speedtest.net server, download,
// upload, then measure latency. The public contract deliberately remains the
// same as the original Fast.com engine so the Bubble Tea UI needs no changes.
func Run(ctx context.Context, p *Progress, connections int, duration time.Duration) {
	if p == nil {
		return
	}
	if connections <= 0 {
		connections = DefaultConnections
	}
	if duration <= 0 {
		duration = DefaultDuration
	}

	runSpeedTest(
		ctx,
		p,
		duration,
		connectedPause,
		newSpeedtestBackend(connections, duration),
	)
}

func runSpeedTest(
	ctx context.Context,
	p *Progress,
	duration time.Duration,
	pause time.Duration,
	backend speedBackend,
) {
	var measurements measurementState

	// Preserve the old engine's best-effort contract: even a cancelled or
	// partially failed run emits PhaseDone and one final Result.
	defer func() {
		sendPhase(p, PhaseDone)
		select {
		case p.Result <- measurements.snapshot():
		default:
		}
	}()

	sendPhase(p, PhaseFinding)
	server, err := backend.FindServer(ctx)
	if err != nil {
		setProgressError(ctx, p, "could not find a Speedtest.net server", err)
		return
	}

	if target := strings.TrimSpace(server.TargetURL()); target != "" {
		p.URLs = []string{target}
	}
	p.ServerName = strings.TrimSpace(server.DisplayName())

	backend.SetDownloadCallback(func(rate float64, total uint64) {
		rate = validRate(rate)
		measurements.record(PhaseDownload, rate)
		sendSample(p, Sample{
			Phase: PhaseDownload,
			Bytes: total,
			Rate:  rate,
			At:    time.Now(),
		})
	})
	backend.SetUploadCallback(func(rate float64, total uint64) {
		rate = validRate(rate)
		measurements.record(PhaseUpload, rate)
		sendSample(p, Sample{
			Phase: PhaseUpload,
			Bytes: total,
			Rate:  rate,
			At:    time.Now(),
		})
	})

	sendPhase(p, PhaseConnected)
	if !waitFor(ctx, pause) {
		return
	}

	sendPhase(p, PhaseDownload)
	started := time.Now()
	if err := server.Download(ctx); err != nil {
		setProgressError(ctx, p, "download test failed", err)
		return
	}
	downloadBytes := backend.TotalDownload()
	downloadMbps := resultMbps(server.DownloadMbps(), downloadBytes, time.Since(started), duration)
	if downloadMbps <= 0 && downloadBytes == 0 {
		setProgressError(ctx, p, "download test failed", errors.New("server transferred no data"))
		return
	}
	measurements.finishDownload(downloadMbps)
	if ctx.Err() != nil {
		return
	}

	sendPhase(p, PhaseUpload)
	started = time.Now()
	if err := server.Upload(ctx); err != nil {
		setProgressError(ctx, p, "upload test failed", err)
		return
	}
	uploadBytes := backend.TotalUpload()
	uploadMbps := resultMbps(server.UploadMbps(), uploadBytes, time.Since(started), duration)
	if uploadMbps <= 0 && uploadBytes == 0 {
		setProgressError(ctx, p, "upload test failed", errors.New("server transferred no data"))
		return
	}
	measurements.finishUpload(uploadMbps)
	if ctx.Err() != nil {
		return
	}

	sendPhase(p, PhaseLatency)
	pingCtx, cancelPing := context.WithTimeout(ctx, latencyTimeout)
	latency, err := server.Ping(pingCtx)
	cancelPing()
	if err != nil {
		setProgressError(ctx, p, "latency test failed", err)
		return
	}
	measurements.finishPing(latency)
}

// measurementState is written by speedtest-go's sampling goroutine while Run
// reads final values. The mutex keeps race-enabled tests and real restarts safe.
type measurementState struct {
	mu     sync.Mutex
	result Result
}

func (m *measurementState) record(phase Phase, rateBytesPerSec float64) {
	mbps := BytesPerSecToMbps(rateBytesPerSec)
	m.mu.Lock()
	defer m.mu.Unlock()

	switch phase {
	case PhaseDownload:
		if mbps > m.result.DownloadPeak {
			m.result.DownloadPeak = mbps
		}
	case PhaseUpload:
		if mbps > m.result.UploadPeak {
			m.result.UploadPeak = mbps
		}
	}
}

func (m *measurementState) finishDownload(mbps float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.result.DownloadMbps = validMbps(mbps)
	if m.result.DownloadPeak < m.result.DownloadMbps {
		m.result.DownloadPeak = m.result.DownloadMbps
	}
}

func (m *measurementState) finishUpload(mbps float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.result.UploadMbps = validMbps(mbps)
	if m.result.UploadPeak < m.result.UploadMbps {
		m.result.UploadPeak = m.result.UploadMbps
	}
}

func (m *measurementState) finishPing(latency time.Duration) {
	ms := float64(latency.Microseconds()) / 1000
	if ms < 0 || math.IsNaN(ms) || math.IsInf(ms, 0) {
		ms = 0
	}
	m.mu.Lock()
	m.result.PingMs = ms
	m.mu.Unlock()
}

func (m *measurementState) snapshot() Result {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.result
}

func resultMbps(reported float64, bytes uint64, elapsed, configured time.Duration) float64 {
	if mbps := validMbps(reported); mbps > 0 {
		return mbps
	}

	// speedtest-go normally supplies an EWMA result. If a provider returns no
	// final value after moving data, retain a useful best-effort average.
	if elapsed <= 0 {
		elapsed = configured
	}
	return bytesToMbps(bytes, elapsed)
}

func validRate(rate float64) float64 {
	if rate < 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return 0
	}
	return rate
}

func validMbps(mbps float64) float64 {
	if mbps < 0 || math.IsNaN(mbps) || math.IsInf(mbps, 0) {
		return 0
	}
	return mbps
}

func setProgressError(ctx context.Context, p *Progress, label string, err error) {
	if err == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return
	}
	p.Err = fmt.Errorf("%s: %w", label, err)
}

func waitFor(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func serverDisplayName(name, country string) string {
	name = strings.TrimSpace(name)
	country = strings.TrimSpace(country)
	switch {
	case name != "" && country != "":
		return name + ", " + country
	case name != "":
		return name
	default:
		return country
	}
}

// sendPhase is deliberately non-blocking. If the UI is momentarily not
// draining (or its buffer is full), a later transition still keeps the engine
// from stalling.
func sendPhase(p *Progress, phase Phase) {
	select {
	case p.Phases <- phase:
	default:
	}
}

// sendSample delivers one instantaneous speed sample to the live UI bridge.
func sendSample(p *Progress, sample Sample) bool {
	select {
	case p.Samples <- sample:
		return true
	default:
		return false
	}
}

func bytesToMbps(bytes uint64, duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	return BytesPerSecToMbps(float64(bytes) / duration.Seconds())
}

// BytesPerSecToMbps converts a raw byte/sec rate into megabits per second.
func BytesPerSecToMbps(bytesPerSec float64) float64 {
	const bitsPerByte = 8
	const mega = 1_000_000
	return (bytesPerSec * bitsPerByte) / mega
}
