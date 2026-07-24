package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/showwin/speedtest-go/speedtest"
)

type speedtestNetBackend struct {
	client *speedtest.Speedtest
}

func newSpeedtestBackend(connections int, duration time.Duration) speedBackend {
	client := speedtest.New(speedtest.WithUserConfig(&speedtest.UserConfig{
		MaxConnections: connections,
		PingMode:       speedtest.HTTP,
	}))
	client.
		SetCaptureTime(duration).
		SetRateCaptureFrequency(sampleInterval)

	return &speedtestNetBackend{client: client}
}

func (b *speedtestNetBackend) FindServer(ctx context.Context) (speedServer, error) {
	servers, err := b.client.FetchServerListContext(ctx)
	if err != nil {
		return nil, err
	}
	available := servers.Available()
	if available.Len() == 0 {
		return nil, fmt.Errorf("no reachable servers in the provider response")
	}
	targets, err := available.FindServer(nil)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 || targets[0] == nil {
		return nil, fmt.Errorf("provider returned no usable servers")
	}
	return &speedtestNetServer{server: targets[0]}, nil
}

func (b *speedtestNetBackend) SetDownloadCallback(callback func(float64, uint64)) {
	b.client.SetCallbackDownload(func(rate speedtest.ByteRate) {
		callback(float64(rate), b.TotalDownload())
	})
}

func (b *speedtestNetBackend) SetUploadCallback(callback func(float64, uint64)) {
	b.client.SetCallbackUpload(func(rate speedtest.ByteRate) {
		callback(float64(rate), b.TotalUpload())
	})
}

func (b *speedtestNetBackend) TotalDownload() uint64 {
	return nonNegativeTotal(b.client.GetTotalDownload())
}

func (b *speedtestNetBackend) TotalUpload() uint64 {
	return nonNegativeTotal(b.client.GetTotalUpload())
}

func nonNegativeTotal(total int64) uint64 {
	if total <= 0 {
		return 0
	}
	return uint64(total)
}

type speedtestNetServer struct {
	server *speedtest.Server
}

func (s *speedtestNetServer) TargetURL() string {
	return s.server.URL
}

func (s *speedtestNetServer) DisplayName() string {
	return serverDisplayName(s.server.Name, s.server.Country)
}

func (s *speedtestNetServer) Download(ctx context.Context) error {
	return s.server.DownloadTestContext(ctx)
}

func (s *speedtestNetServer) Upload(ctx context.Context) error {
	return s.server.UploadTestContext(ctx)
}

func (s *speedtestNetServer) Ping(ctx context.Context) (time.Duration, error) {
	if err := s.server.PingTestContext(ctx, nil); err != nil {
		return 0, err
	}
	return s.server.Latency, nil
}

func (s *speedtestNetServer) DownloadMbps() float64 {
	return s.server.DLSpeed.Mbps()
}

func (s *speedtestNetServer) UploadMbps() float64 {
	return s.server.ULSpeed.Mbps()
}
