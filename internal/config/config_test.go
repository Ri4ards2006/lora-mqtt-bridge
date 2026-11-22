package config

import (
	"strings"
	"testing"
)

func TestLoadFromEnvMissingRequired(t *testing.T) {
	for _, k := range []string{"MQTT_BROKER", "INFLUX_URL", "INFLUX_TOKEN", "INFLUX_ORG", "INFLUX_BUCKET"} {
		t.Setenv(k, "")
	}

	_, err := LoadFromEnv()
	if err == nil {
		t.Fatal("expected error when required env vars are missing")
	}
	for _, k := range []string{"MQTT_BROKER", "INFLUX_URL", "INFLUX_TOKEN", "INFLUX_ORG", "INFLUX_BUCKET"} {
		if !strings.Contains(err.Error(), k) {
			t.Errorf("expected error to mention %s, got: %v", k, err)
		}
	}
}

func TestLoadFromEnvValid(t *testing.T) {
	t.Setenv("MQTT_BROKER", "tcp://mqtt.example.local:1883")
	t.Setenv("MQTT_TOPIC", "msh/EU_868/#")
	t.Setenv("MQTT_USERNAME", "meshtastic")
	t.Setenv("MQTT_PASSWORD", "secret-pass")
	t.Setenv("INFLUX_URL", "http://influxdb.example.local:8086")
	t.Setenv("INFLUX_TOKEN", "dummy-token")
	t.Setenv("INFLUX_ORG", "homelab")
	t.Setenv("INFLUX_BUCKET", "meshtastic")
	t.Setenv("WORKERS", "4")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MQTTBroker != "tcp://mqtt.example.local:1883" {
		t.Errorf("unexpected MQTTBroker: %q", cfg.MQTTBroker)
	}
	if cfg.MQTTUsername != "meshtastic" || cfg.MQTTPassword != "secret-pass" {
		t.Errorf("unexpected MQTT credentials")
	}
	if cfg.InfluxURL != "http://influxdb.example.local:8086" || cfg.InfluxToken != "dummy-token" {
		t.Errorf("unexpected Influx settings")
	}
	if cfg.InfluxOrg != "homelab" || cfg.InfluxBucket != "meshtastic" {
		t.Errorf("unexpected Influx org/bucket")
	}
	if cfg.NumWorkers != 4 {
		t.Errorf("expected 4 workers, got %d", cfg.NumWorkers)
	}
}
