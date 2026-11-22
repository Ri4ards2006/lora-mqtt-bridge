package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Ri4ards2006/lora-mqtt-bridge/internal/parser"
)

// RawMessage represents an incoming MQTT payload queued for worker processing.
type RawMessage struct {
	Topic      string
	Payload    []byte
	ReceivedAt time.Time
}

// Sink consumes parsed TelemetryMetric records (e.g., InfluxDB v2 batch writer).
type Sink interface {
	Write(ctx context.Context, metric parser.TelemetryMetric) error
	Close() error
}

// EngineConfig mirrors GO-CORE-LAB's pipeline.EngineConfig.
type EngineConfig struct {
	NumWorkers         int
	QueueSize          int
	DropOnBackpressure bool
}

// Stats holds lock-free atomic telemetry counters for observability.
type Stats struct {
	Received  uint64 `json:"received"`
	Processed uint64 `json:"processed"`
	Ignored   uint64 `json:"ignored"`
	Dropped   uint64 `json:"dropped"`
	ParseErrs uint64 `json:"parse_errors"`
	SinkErrs  uint64 `json:"sink_errors"`
	QueueLen  int    `json:"queue_depth"`
}

// Engine implements the concurrent worker-pool pipeline (analogous to GO-CORE-LAB).
type Engine struct {
	cfg    EngineConfig
	sinks  []Sink
	logger *slog.Logger

	inChan chan RawMessage
	wg     sync.WaitGroup

	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	started bool
	stopped bool

	received  atomic.Uint64
	processed atomic.Uint64
	ignored   atomic.Uint64
	dropped   atomic.Uint64
	parseErrs atomic.Uint64
	sinkErrs  atomic.Uint64
}

// NewEngine constructs a concurrent worker-pool engine.
func NewEngine(cfg EngineConfig, logger *slog.Logger, sinks ...Sink) *Engine {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = runtime.NumCPU()
		if cfg.NumWorkers < 2 {
			cfg.NumWorkers = 2
		}
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 2048
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Engine{
		cfg:    cfg,
		sinks:  sinks,
		logger: logger,
		inChan: make(chan RawMessage, cfg.QueueSize),
	}
}

// Start launches the worker pool goroutines.
func (e *Engine) Start(parentCtx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started {
		return
	}
	e.ctx, e.cancel = context.WithCancel(parentCtx)
	e.started = true

	for i := 0; i < e.cfg.NumWorkers; i++ {
		e.wg.Add(1)
		go e.workerLoop(i)
	}
	e.logger.Info("pipeline worker pool started",
		"workers", e.cfg.NumWorkers,
		"queue_size", e.cfg.QueueSize,
		"drop_on_backpressure", e.cfg.DropOnBackpressure,
	)
}

// Submit enqueues a raw MQTT message into the worker pool.
// Safe to call concurrently from paho MQTT callbacks.
func (e *Engine) Submit(msg RawMessage) bool {
	e.mu.Lock()
	if !e.started || e.stopped {
		e.mu.Unlock()
		return false
	}
	ctx := e.ctx
	e.mu.Unlock()

	e.received.Add(1)

	if e.cfg.DropOnBackpressure {
		select {
		case e.inChan <- msg:
			return true
		default:
			e.dropped.Add(1)
			return false
		}
	}

	select {
	case e.inChan <- msg:
		return true
	case <-ctx.Done():
		e.dropped.Add(1)
		return false
	}
}

func (e *Engine) workerLoop(workerID int) {
	defer e.wg.Done()

	for msg := range e.inChan {
		metric, err := parser.ParseMeshtastic(msg.Topic, msg.Payload, msg.ReceivedAt)
		if err != nil {
			if errors.Is(err, parser.ErrIgnoreTopic) {
				e.ignored.Add(1)
				continue
			}
			e.parseErrs.Add(1)
			e.logger.Debug("payload parse error",
				"worker", workerID,
				"topic", msg.Topic,
				"err", err,
			)
			continue
		}

		for _, s := range e.sinks {
			if err := s.Write(e.ctx, metric); err != nil {
				e.sinkErrs.Add(1)
				e.logger.Warn("sink write error",
					"worker", workerID,
					"node_id", metric.NodeID,
					"err", err,
				)
			}
		}
		e.processed.Add(1)
	}
}

// Snapshot returns a point-in-time copy of pipeline counters.
func (e *Engine) Snapshot() Stats {
	return Stats{
		Received:  e.received.Load(),
		Processed: e.processed.Load(),
		Ignored:   e.ignored.Load(),
		Dropped:   e.dropped.Load(),
		ParseErrs: e.parseErrs.Load(),
		SinkErrs:  e.sinkErrs.Load(),
		QueueLen:  len(e.inChan),
	}
}

// Stop drains the queue, waits for all workers to finish, and flushes/closes all sinks.
// Callers must stop upstream MQTT ingestion before calling Stop().
func (e *Engine) Stop() error {
	e.mu.Lock()
	if !e.started || e.stopped {
		e.mu.Unlock()
		return nil
	}
	e.stopped = true
	close(e.inChan)
	e.mu.Unlock()

	// Wait for workers to finish draining queued messages before cancelling context
	e.wg.Wait()

	e.mu.Lock()
	if e.cancel != nil {
		e.cancel()
	}
	e.mu.Unlock()

	var firstErr error
	for _, s := range e.sinks {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
