package sink

import (
	"context"
	"log/slog"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"

	"github.com/Ri4ards2006/lora-mqtt-bridge/internal/parser"
)

// InfluxConfig configures the InfluxDB v2 batching sink.
type InfluxConfig struct {
	URL        string
	Token      string
	Org        string
	Bucket     string
	BatchSize  uint
	FlushEvery time.Duration
}

// InfluxSink transforms TelemetryMetric records into InfluxDB v2 Points
// and writes them via the asynchronous batched WriteAPI.
type InfluxSink struct {
	client   influxdb2.Client
	writeAPI api.WriteAPI
	logger   *slog.Logger
	errDone  chan struct{}
	closeMu  sync.Mutex
	closed   bool
}

// NewInfluxSink initializes an InfluxDB v2 client and background batch writer.
func NewInfluxSink(cfg InfluxConfig, logger *slog.Logger) *InfluxSink {
	if logger == nil {
		logger = slog.Default()
	}
	flushMs := uint(cfg.FlushEvery.Milliseconds())
	if flushMs == 0 {
		flushMs = 5000
	}

	opts := influxdb2.DefaultOptions().
		SetBatchSize(cfg.BatchSize).
		SetFlushInterval(flushMs).
		SetUseGZip(true).
		SetPrecision(time.Second)

	client := influxdb2.NewClientWithOptions(cfg.URL, cfg.Token, opts)
	writeAPI := client.WriteAPI(cfg.Org, cfg.Bucket)

	s := &InfluxSink{
		client:   client,
		writeAPI: writeAPI,
		logger:   logger,
		errDone:  make(chan struct{}),
	}

	// Drain async write errors so the error channel never blocks.
	go func() {
		defer close(s.errDone)
		for err := range writeAPI.Errors() {
			s.logger.Error("influxdb async batch write failed", "err", err)
		}
	}()

	return s
}

// MetricToPoint converts a normalized TelemetryMetric into an InfluxDB v2 Point.
func MetricToPoint(m parser.TelemetryMetric) *write.Point {
	tags := map[string]string{
		"node_id":     defaultTag(m.NodeID, "unknown"),
		"gateway_id":  defaultTag(m.GatewayID, "unknown"),
		"channel":     defaultTag(m.Channel, "unknown"),
		"packet_type": defaultTag(m.PacketType, "unknown"),
		"region":      defaultTag(m.Region, "unknown"),
		"format":      defaultTag(m.Format, "unknown"),
	}

	fields := map[string]interface{}{
		"packet_count": int64(1),
		"encrypted":    m.Encrypted,
	}
	if m.HasSNR {
		fields["snr"] = m.SNR
	}
	if m.HasRSSI {
		fields["rssi"] = m.RSSI
	}
	if m.HasHops {
		fields["hops"] = m.Hops
	}
	if m.HasBattery {
		fields["battery_level"] = int64(m.BatteryLevel)
	}
	if m.HasVoltage {
		fields["voltage"] = m.Voltage
	}
	if m.HasChannelUtilization {
		fields["channel_utilization"] = m.ChannelUtilization
	}
	if m.HasAirUtilTx {
		fields["air_util_tx"] = m.AirUtilTx
	}
	if m.HasTemperature {
		fields["temperature"] = m.Temperature
	}
	if m.HasHumidity {
		fields["relative_humidity"] = m.Humidity
	}
	if m.HasPressure {
		fields["barometric_pressure"] = m.Pressure
	}

	ts := m.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	return influxdb2.NewPoint("meshtastic_telemetry", tags, fields, ts)
}

// Write enqueues a metric point into the InfluxDB batch buffer.
func (s *InfluxSink) Write(_ context.Context, m parser.TelemetryMetric) error {
	p := MetricToPoint(m)
	s.writeAPI.WritePoint(p)
	return nil
}

// Ping verifies connectivity to InfluxDB v2.
func (s *InfluxSink) Ping(ctx context.Context) error {
	_, err := s.client.Ping(ctx)
	return err
}

// Close flushes all pending batched points and closes the InfluxDB client.
func (s *InfluxSink) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closed = true
	s.closeMu.Unlock()

	s.writeAPI.Flush()
	s.client.Close()
	<-s.errDone
	return nil
}

func defaultTag(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
