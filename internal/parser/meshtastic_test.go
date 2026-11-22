package parser

import (
	"errors"
	"math"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

func TestParseMeshtasticJSONTelemetry(t *testing.T) {
	raw := []byte(`{
		"channel": 0,
		"from": 2712847316,
		"to": 4294967295,
		"id": 987654,
		"sender": "!deadbeef",
		"timestamp": 1732300000,
		"type": "telemetry",
		"rssi": -92,
		"snr": 6.75,
		"hop_start": 3,
		"hop_limit": 2,
		"payload": {
			"battery_level": 87,
			"voltage": 4.08,
			"channel_utilization": 4.2,
			"air_util_tx": 0.9
		}
	}`)

	now := time.Now().UTC()
	m, err := ParseMeshtastic("msh/EU_868/2/json/LongFast/!deadbeef", raw, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if m.Region != "EU_868" || m.Channel != "LongFast" || m.GatewayID != "!deadbeef" {
		t.Errorf("unexpected topic meta: region=%q channel=%q gw=%q", m.Region, m.Channel, m.GatewayID)
	}
	if m.NodeID != "!a1b2c3d4" {
		t.Errorf("expected node !a1b2c3d4, got %q", m.NodeID)
	}
	if !m.HasSNR || m.SNR != 6.75 {
		t.Errorf("unexpected SNR: has=%v val=%v", m.HasSNR, m.SNR)
	}
	if !m.HasRSSI || m.RSSI != -92 {
		t.Errorf("unexpected RSSI: has=%v val=%v", m.HasRSSI, m.RSSI)
	}
	if !m.HasHops || m.Hops != 1 {
		t.Errorf("expected hops=1, got has=%v val=%v", m.HasHops, m.Hops)
	}
	if !m.HasBattery || m.BatteryLevel != 87 {
		t.Errorf("expected battery=87, got has=%v val=%v", m.HasBattery, m.BatteryLevel)
	}
	if !m.HasVoltage || math.Abs(m.Voltage-4.08) > 1e-6 {
		t.Errorf("unexpected voltage: %v", m.Voltage)
	}
}

func TestParseMeshtasticProtobufTelemetry(t *testing.T) {
	// Build DeviceMetrics { battery_level=91, voltage=4.12 }
	var dev []byte
	dev = protowire.AppendTag(dev, 1, protowire.VarintType)
	dev = protowire.AppendVarint(dev, 91)
	dev = protowire.AppendTag(dev, 2, protowire.Fixed32Type)
	dev = protowire.AppendFixed32(dev, math.Float32bits(4.12))

	// Telemetry { device_metrics = dev }
	var tel []byte
	tel = protowire.AppendTag(tel, 2, protowire.BytesType)
	tel = protowire.AppendBytes(tel, dev)

	// Data { portnum=67 (TELEMETRY_APP), payload=tel }
	var data []byte
	data = protowire.AppendTag(data, 1, protowire.VarintType)
	data = protowire.AppendVarint(data, 67)
	data = protowire.AppendTag(data, 2, protowire.BytesType)
	data = protowire.AppendBytes(data, tel)

	// MeshPacket { from=0x11223344, decoded=data, rx_snr=-3.5, hop_limit=1, rx_rssi=-108, hop_start=3 }
	var pkt []byte
	pkt = protowire.AppendTag(pkt, 1, protowire.Fixed32Type)
	pkt = protowire.AppendFixed32(pkt, 0x11223344)
	pkt = protowire.AppendTag(pkt, 4, protowire.BytesType)
	pkt = protowire.AppendBytes(pkt, data)
	pkt = protowire.AppendTag(pkt, 8, protowire.Fixed32Type)
	pkt = protowire.AppendFixed32(pkt, math.Float32bits(-3.5))
	pkt = protowire.AppendTag(pkt, 9, protowire.VarintType)
	pkt = protowire.AppendVarint(pkt, 1)
	pkt = protowire.AppendTag(pkt, 12, protowire.VarintType)
	rssiNeg := int32(-108)
	pkt = protowire.AppendVarint(pkt, uint64(uint32(rssiNeg)))
	pkt = protowire.AppendTag(pkt, 15, protowire.VarintType)
	pkt = protowire.AppendVarint(pkt, 3)

	// ServiceEnvelope { packet=pkt, channel_id="LongFast", gateway_id="!cafebabe" }
	var env []byte
	env = protowire.AppendTag(env, 1, protowire.BytesType)
	env = protowire.AppendBytes(env, pkt)
	env = protowire.AppendTag(env, 2, protowire.BytesType)
	env = protowire.AppendString(env, "LongFast")
	env = protowire.AppendTag(env, 3, protowire.BytesType)
	env = protowire.AppendString(env, "!cafebabe")

	m, err := ParseMeshtastic("msh/EU_868/2/e/LongFast/!cafebabe", env, time.Now().UTC())
	if err != nil {
		t.Fatalf("unexpected protobuf error: %v", err)
	}

	if m.Format != "protobuf" || m.PacketType != "telemetry" {
		t.Errorf("expected protobuf/telemetry, got %s/%s", m.Format, m.PacketType)
	}
	if m.NodeID != "!11223344" || m.GatewayID != "!cafebabe" {
		t.Errorf("unexpected ids: node=%q gw=%q", m.NodeID, m.GatewayID)
	}
	if !m.HasSNR || math.Abs(m.SNR-(-3.5)) > 1e-4 {
		t.Errorf("expected SNR -3.5, got %v", m.SNR)
	}
	if !m.HasRSSI || m.RSSI != -108 {
		t.Errorf("expected RSSI -108, got %v", m.RSSI)
	}
	if !m.HasHops || m.Hops != 2 {
		t.Errorf("expected hops=2, got %v", m.Hops)
	}
	if !m.HasBattery || m.BatteryLevel != 91 {
		t.Errorf("expected battery=91, got %v", m.BatteryLevel)
	}
}

func TestIgnoreStatTopic(t *testing.T) {
	_, err := ParseMeshtastic("msh/EU_868/2/stat/!deadbeef", []byte("online"), time.Now().UTC())
	if !errors.Is(err, ErrIgnoreTopic) {
		t.Fatalf("expected ErrIgnoreTopic, got %v", err)
	}
}
