package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Ri4ards2006/lora-mqtt-bridge/internal/config"
	"github.com/Ri4ards2006/lora-mqtt-bridge/internal/mqtt"
	"github.com/Ri4ards2006/lora-mqtt-bridge/internal/pipeline"
	"github.com/Ri4ards2006/lora-mqtt-bridge/internal/sink"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg, err := config.LoadFromEnv()
	if err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	// 1. Initialize InfluxDB v2 batching sink
	influxSink := sink.NewInfluxSink(sink.InfluxConfig{
		URL:        cfg.InfluxURL,
		Token:      cfg.InfluxToken,
		Org:        cfg.InfluxOrg,
		Bucket:     cfg.InfluxBucket,
		BatchSize:  cfg.BatchSize,
		FlushEvery: cfg.FlushEvery,
	}, logger)

	// 2. Start concurrent worker-pool pipeline (GO-CORE-LAB architecture)
	engine := pipeline.NewEngine(pipeline.EngineConfig{
		NumWorkers:         cfg.NumWorkers,
		QueueSize:          cfg.QueueSize,
		DropOnBackpressure: cfg.DropOnBackpressure,
	}, logger, influxSink)
	engine.Start(ctx)

	// 3. Connect MQTT subscriber
	mqttClient := mqtt.NewClient(mqtt.ClientConfig{
		Broker:   cfg.MQTTBroker,
		Topic:    cfg.MQTTTopic,
		ClientID: cfg.MQTTClientID,
		Username: cfg.MQTTUsername,
		Password: cfg.MQTTPassword,
		QoS:      cfg.MQTTQoS,
	}, func(topic string, payload []byte, receivedAt time.Time) {
		engine.Submit(pipeline.RawMessage{
			Topic:      topic,
			Payload:    payload,
			ReceivedAt: receivedAt,
		})
	}, logger)

	// 4. Start lightweight HTTP server for K3s 3-stage health probes & live stats
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !mqttClient.IsConnected() {
			http.Error(w, "mqtt not connected", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"mqtt_connected": mqttClient.IsConnected(),
			"pipeline":       engine.Snapshot(),
		})
	})

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Info("health server listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server error", "err", err)
		}
	}()

	// Connect to MQTT broker (non-fatal retry loop handled by paho SetConnectRetry)
	if err := mqttClient.Connect(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("initial mqtt connect returned error; background retry active", "err", err)
	}

	logger.Info("lora-mqtt-bridge running",
		"mqtt_broker", cfg.MQTTBroker,
		"mqtt_topic", cfg.MQTTTopic,
		"influx_url", cfg.InfluxURL,
		"influx_bucket", cfg.InfluxBucket,
	)

	// Block until SIGINT / SIGTERM
	<-ctx.Done()
	logger.Info("shutdown signal received, draining pipeline")

	// Graceful shutdown order:
	// 1) Stop MQTT ingestion so no new messages arrive
	mqttClient.Close()

	// 2) Drain worker pool and flush InfluxDB batch buffer
	if err := engine.Stop(); err != nil {
		logger.Error("pipeline stop error", "err", err)
	}

	// 3) Shut down HTTP probe server
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)

	snap := engine.Snapshot()
	logger.Info("lora-mqtt-bridge stopped cleanly",
		"received", snap.Received,
		"processed", snap.Processed,
		"dropped", snap.Dropped,
		"parse_errors", snap.ParseErrs,
		"sink_errors", snap.SinkErrs,
	)
}
