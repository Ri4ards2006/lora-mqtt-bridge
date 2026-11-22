package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Ri4ards2006/lora-mqtt-bridge/internal/parser"
)

type memSink struct {
	mu      sync.Mutex
	metrics []parser.TelemetryMetric
	closed  bool
}

func (m *memSink) Write(_ context.Context, metric parser.TelemetryMetric) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics = append(m.metrics, metric)
	return nil
}

func (m *memSink) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func TestEngineConcurrentProcessingAndGracefulShutdown(t *testing.T) {
	s := &memSink{}
	eng := NewEngine(EngineConfig{
		NumWorkers:         4,
		QueueSize:          128,
		DropOnBackpressure: false,
	}, nil, s)

	ctx := context.Background()
	eng.Start(ctx)

	payload := []byte(`{"from":2712847316,"type":"telemetry","rssi":-85,"snr":8.5,"hop_start":3,"hop_limit":3,"payload":{"battery_level":95}}`)
	const total = 50
	for i := 0; i < total; i++ {
		if !eng.Submit(RawMessage{
			Topic:      "msh/EU_868/2/json/LongFast/!gw01",
			Payload:    payload,
			ReceivedAt: time.Now().UTC(),
		}) {
			t.Fatalf("submit %d failed", i)
		}
	}

	if err := eng.Stop(); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	snap := eng.Snapshot()
	if snap.Processed != total {
		t.Fatalf("expected %d processed, got %d (parseErr=%d, drop=%d)", total, snap.Processed, snap.ParseErrs, snap.Dropped)
	}
	if !s.closed {
		t.Fatal("expected sink to be closed on Stop()")
	}
	if len(s.metrics) != total {
		t.Fatalf("expected %d sink writes, got %d", total, len(s.metrics))
	}
}
