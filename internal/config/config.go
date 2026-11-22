package config

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime configuration for lora-mqtt-bridge.
// All connection endpoints and credentials are strictly sourced from environment variables.
type Config struct {
	// MQTT
	MQTTBroker   string
	MQTTTopic    string
	MQTTClientID string
	MQTTUsername string
	MQTTPassword string
	MQTTQoS      byte

	// InfluxDB v2
	InfluxURL    string
	InfluxToken  string
	InfluxOrg    string
	InfluxBucket string
	BatchSize    uint
	FlushEvery   time.Duration

	// Worker Pool (mirrors GO-CORE-LAB EngineConfig)
	NumWorkers         int
	QueueSize          int
	DropOnBackpressure bool

	// HTTP Health / Stats Server
	HTTPAddr string
}

// LoadFromEnv loads configuration strictly from environment variables.
// Returns a descriptive error listing all missing required variables so callers can fail fast.
func LoadFromEnv() (Config, error) {
	defaultWorkers := runtime.NumCPU()
	if defaultWorkers < 2 {
		defaultWorkers = 2
	}

	cfg := Config{
		MQTTBroker:         strings.TrimSpace(os.Getenv("MQTT_BROKER")),
		MQTTTopic:          getEnvDefault("MQTT_TOPIC", "msh/EU_868/#"),
		MQTTClientID:       getEnvDefault("MQTT_CLIENT_ID", "lora-mqtt-bridge"),
		MQTTUsername:       os.Getenv("MQTT_USERNAME"),
		MQTTPassword:       os.Getenv("MQTT_PASSWORD"),
		MQTTQoS:            byte(getEnvInt("MQTT_QOS", 1)),
		InfluxURL:          strings.TrimSpace(os.Getenv("INFLUX_URL")),
		InfluxToken:        strings.TrimSpace(os.Getenv("INFLUX_TOKEN")),
		InfluxOrg:          strings.TrimSpace(os.Getenv("INFLUX_ORG")),
		InfluxBucket:       strings.TrimSpace(os.Getenv("INFLUX_BUCKET")),
		BatchSize:          uint(getEnvInt("INFLUX_BATCH_SIZE", 50)),
		FlushEvery:         getEnvDuration("INFLUX_FLUSH_INTERVAL", 5*time.Second),
		NumWorkers:         getEnvInt("WORKERS", defaultWorkers),
		QueueSize:          getEnvInt("QUEUE_SIZE", 2048),
		DropOnBackpressure: getEnvBool("DROP_ON_BACKPRESSURE", true),
		HTTPAddr:           getEnvDefault("HTTP_ADDR", ":8080"),
	}

	var missing []string
	if cfg.MQTTBroker == "" {
		missing = append(missing, "MQTT_BROKER")
	}
	if cfg.InfluxURL == "" {
		missing = append(missing, "INFLUX_URL")
	}
	if cfg.InfluxToken == "" {
		missing = append(missing, "INFLUX_TOKEN")
	}
	if cfg.InfluxOrg == "" {
		missing = append(missing, "INFLUX_ORG")
	}
	if cfg.InfluxBucket == "" {
		missing = append(missing, "INFLUX_BUCKET")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = defaultWorkers
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1024
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 50
	}
	return cfg, nil
}

func getEnvDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
