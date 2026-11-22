package parser

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

// ErrIgnoreTopic indicates a non-packet MQTT topic (e.g. stat/online status) that should be skipped.
var ErrIgnoreTopic = errors.New("non-packet meshtastic topic")

// TelemetryMetric is the normalized output extracted from either Meshtastic JSON
// or Protobuf ServiceEnvelope MQTT messages.
type TelemetryMetric struct {
	Timestamp   time.Time
	Region      string
	Channel     string
	GatewayID   string
	NodeID      string
	DestID      string
	PacketID    uint32
	PacketType  string
	Format      string // "json" or "protobuf"
	Encrypted   bool

	// RF / Mesh Metrics
	HasSNR  bool
	SNR     float64
	HasRSSI bool
	RSSI    int64
	HasHops bool
	Hops    int64

	// Device Telemetry Metrics (optional, present on TELEMETRY_APP)
	HasBattery            bool
	BatteryLevel          uint32
	HasVoltage            bool
	Voltage               float64
	HasChannelUtilization bool
	ChannelUtilization    float64
	HasAirUtilTx          bool
	AirUtilTx             float64

	// Environment Telemetry (optional)
	HasTemperature bool
	Temperature    float64
	HasHumidity    bool
	Humidity       float64
	HasPressure    bool
	Pressure       float64
}

// ParseMeshtastic parses an incoming MQTT message from a Meshtastic gateway.
// Supported topic shapes:
//   - msh/<region>/2/json/<channel>/<gateway_id>
//   - msh/<region>/2/e/<channel>/<gateway_id>   (Protobuf ServiceEnvelope)
//   - msh/<region>/2/stat/<gateway_id>          (ignored via ErrIgnoreTopic)
func ParseMeshtastic(topic string, payload []byte, receivedAt time.Time) (TelemetryMetric, error) {
	if len(payload) == 0 {
		return TelemetryMetric{}, errors.New("empty payload")
	}

	region, mode, channel, gatewayID := parseTopicMeta(topic)
	if mode == "stat" || mode == "map" {
		return TelemetryMetric{}, ErrIgnoreTopic
	}

	// Auto-detect JSON vs Protobuf based on topic segment or leading '{'
	if mode == "json" || payload[0] == '{' {
		m, err := parseJSON(payload, receivedAt)
		if err != nil {
			return TelemetryMetric{}, err
		}
		if m.Region == "" {
			m.Region = region
		}
		if m.Channel == "" {
			m.Channel = channel
		}
		if m.GatewayID == "" {
			m.GatewayID = gatewayID
		}
		return m, nil
	}

	m, err := parseProtobufEnvelope(payload, receivedAt)
	if err != nil {
		return TelemetryMetric{}, err
	}
	if m.Region == "" {
		m.Region = region
	}
	if m.Channel == "" {
		m.Channel = channel
	}
	if m.GatewayID == "" {
		m.GatewayID = gatewayID
	}
	return m, nil
}

// parseTopicMeta extracts (region, mode, channel, gatewayID) from msh/<region>/2/<mode>/<channel>/<gateway>.
func parseTopicMeta(topic string) (region, mode, channel, gatewayID string) {
	parts := strings.Split(topic, "/")
	// Expected: [0]=msh, [1]=EU_868, [2]=2, [3]=json|e|stat, [4]=LongFast, [5]=!gateway
	if len(parts) >= 2 {
		region = parts[1]
	}
	if len(parts) >= 4 {
		mode = parts[3]
	}
	if len(parts) >= 5 {
		channel = parts[4]
	}
	if len(parts) >= 6 {
		gatewayID = parts[5]
	}
	return region, mode, channel, gatewayID
}

// --- JSON Parser ---

type jsonEnvelope struct {
	ID        uint32          `json:"id"`
	From      uint32          `json:"from"`
	To        uint32          `json:"to"`
	Sender    string          `json:"sender"`
	Channel   json.RawMessage `json:"channel"`
	Type      string          `json:"type"`
	Timestamp int64           `json:"timestamp"`
	RSSI      *int64          `json:"rssi"`
	RxRSSI    *int64          `json:"rx_rssi"`
	SNR       *float64        `json:"snr"`
	RxSNR     *float64        `json:"rx_snr"`
	HopStart  *int64          `json:"hop_start"`
	HopLimit  *int64          `json:"hop_limit"`
	HopsAway  *int64          `json:"hops_away"`
	Payload   json.RawMessage `json:"payload"`
}

type jsonTelemetryPayload struct {
	BatteryLevel       *uint32  `json:"battery_level"`
	Voltage            *float64 `json:"voltage"`
	ChannelUtilization *float64 `json:"channel_utilization"`
	AirUtilTx          *float64 `json:"air_util_tx"`
	Temperature        *float64 `json:"temperature"`
	RelativeHumidity   *float64 `json:"relative_humidity"`
	BarometricPressure *float64 `json:"barometric_pressure"`
}

func parseJSON(raw []byte, receivedAt time.Time) (TelemetryMetric, error) {
	var env jsonEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return TelemetryMetric{}, fmt.Errorf("json unmarshal: %w", err)
	}

	ts := receivedAt
	if env.Timestamp > 1_000_000_000 {
		ts = time.Unix(env.Timestamp, 0).UTC()
	}

	m := TelemetryMetric{
		Timestamp:  ts,
		GatewayID:  env.Sender,
		NodeID:     formatNodeID(env.From),
		DestID:     formatNodeID(env.To),
		PacketID:   env.ID,
		PacketType: env.Type,
		Format:     "json",
	}
	if m.PacketType == "" {
		m.PacketType = "unknown"
	}

	// SNR
	if env.RxSNR != nil {
		m.HasSNR, m.SNR = true, *env.RxSNR
	} else if env.SNR != nil {
		m.HasSNR, m.SNR = true, *env.SNR
	}

	// RSSI
	if env.RxRSSI != nil {
		m.HasRSSI, m.RSSI = true, *env.RxRSSI
	} else if env.RSSI != nil {
		m.HasRSSI, m.RSSI = true, *env.RSSI
	}

	// Hops
	if env.HopsAway != nil && *env.HopsAway >= 0 {
		m.HasHops, m.Hops = true, *env.HopsAway
	} else if env.HopStart != nil && env.HopLimit != nil && *env.HopStart > 0 {
		hops := *env.HopStart - *env.HopLimit
		if hops < 0 {
			hops = 0
		}
		m.HasHops, m.Hops = true, hops
	}

	// Telemetry payload fields (works when type == "telemetry" or any object containing device metrics)
	if len(env.Payload) > 0 && env.Payload[0] == '{' {
		var tp jsonTelemetryPayload
		if err := json.Unmarshal(env.Payload, &tp); err == nil {
			if tp.BatteryLevel != nil {
				m.HasBattery, m.BatteryLevel = true, *tp.BatteryLevel
			}
			if tp.Voltage != nil {
				m.HasVoltage, m.Voltage = true, *tp.Voltage
			}
			if tp.ChannelUtilization != nil {
				m.HasChannelUtilization, m.ChannelUtilization = true, *tp.ChannelUtilization
			}
			if tp.AirUtilTx != nil {
				m.HasAirUtilTx, m.AirUtilTx = true, *tp.AirUtilTx
			}
			if tp.Temperature != nil {
				m.HasTemperature, m.Temperature = true, *tp.Temperature
			}
			if tp.RelativeHumidity != nil {
				m.HasHumidity, m.Humidity = true, *tp.RelativeHumidity
			}
			if tp.BarometricPressure != nil {
				m.HasPressure, m.Pressure = true, *tp.BarometricPressure
			}
		}
	}

	return m, nil
}

// --- Lightweight Protobuf Wire Decoder for Meshtastic ServiceEnvelope ---
//
// Meshtastic protobuf wire definitions (meshtastic/mqtt.proto, mesh.proto, telemetry.proto):
//   ServiceEnvelope:
//     1: MeshPacket packet (bytes/message)
//     2: string channel_id
//     3: string gateway_id
//   MeshPacket:
//     1: fixed32 from
//     2: fixed32 to
//     4: Data decoded (message)
//     5: bytes encrypted
//     6: fixed32 id
//     7: fixed32 rx_time
//     8: float rx_snr
//     9: uint32 hop_limit
//    12: int32 rx_rssi
//    15: uint32 hop_start
//   Data:
//     1: PortNum portnum (varint; 67 = TELEMETRY_APP)
//     2: bytes payload
//   Telemetry:
//     1: fixed32 time
//     2: DeviceMetrics device_metrics
//     3: EnvironmentMetrics environment_metrics
//   DeviceMetrics:
//     1: uint32 battery_level
//     2: float voltage
//     3: float channel_utilization
//     4: float air_util_tx

func parseProtobufEnvelope(raw []byte, receivedAt time.Time) (TelemetryMetric, error) {
	m := TelemetryMetric{
		Timestamp:  receivedAt,
		Format:     "protobuf",
		PacketType: "unknown",
	}

	var packetBytes []byte
	b := raw
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return TelemetryMetric{}, fmt.Errorf("protobuf envelope tag: %w", protowire.ParseError(n))
		}
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.BytesType:
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return TelemetryMetric{}, fmt.Errorf("protobuf packet bytes: %w", protowire.ParseError(vn))
			}
			packetBytes = v
			b = b[vn:]
		case num == 2 && typ == protowire.BytesType:
			v, vn := protowire.ConsumeString(b)
			if vn < 0 {
				return TelemetryMetric{}, fmt.Errorf("protobuf channel_id: %w", protowire.ParseError(vn))
			}
			m.Channel = v
			b = b[vn:]
		case num == 3 && typ == protowire.BytesType:
			v, vn := protowire.ConsumeString(b)
			if vn < 0 {
				return TelemetryMetric{}, fmt.Errorf("protobuf gateway_id: %w", protowire.ParseError(vn))
			}
			m.GatewayID = v
			b = b[vn:]
		default:
			vn := protowire.ConsumeFieldValue(num, typ, b)
			if vn < 0 {
				return TelemetryMetric{}, fmt.Errorf("protobuf envelope skip: %w", protowire.ParseError(vn))
			}
			b = b[vn:]
		}
	}

	if len(packetBytes) == 0 {
		return TelemetryMetric{}, errors.New("protobuf ServiceEnvelope missing MeshPacket")
	}

	if err := decodeMeshPacket(packetBytes, &m); err != nil {
		return TelemetryMetric{}, err
	}
	return m, nil
}

func decodeMeshPacket(b []byte, m *TelemetryMetric) error {
	var hopStart, hopLimit uint64
	var hasHopStart, hasHopLimit bool
	var decodedBytes []byte

	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return fmt.Errorf("meshpacket tag: %w", protowire.ParseError(n))
		}
		b = b[n:]

		switch {
		case num == 1 && typ == protowire.Fixed32Type:
			v, vn := protowire.ConsumeFixed32(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			m.NodeID = formatNodeID(v)
			b = b[vn:]
		case num == 2 && typ == protowire.Fixed32Type:
			v, vn := protowire.ConsumeFixed32(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			m.DestID = formatNodeID(v)
			b = b[vn:]
		case num == 4 && typ == protowire.BytesType:
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			decodedBytes = v
			b = b[vn:]
		case num == 5 && typ == protowire.BytesType:
			_, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			m.Encrypted = true
			m.PacketType = "encrypted"
			b = b[vn:]
		case num == 6 && typ == protowire.Fixed32Type:
			v, vn := protowire.ConsumeFixed32(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			m.PacketID = v
			b = b[vn:]
		case num == 7 && typ == protowire.Fixed32Type:
			v, vn := protowire.ConsumeFixed32(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			if v > 1_000_000_000 {
				m.Timestamp = time.Unix(int64(v), 0).UTC()
			}
			b = b[vn:]
		case num == 8 && typ == protowire.Fixed32Type:
			v, vn := protowire.ConsumeFixed32(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			m.HasSNR = true
			m.SNR = float64(math.Float32frombits(v))
			b = b[vn:]
		case num == 9 && typ == protowire.VarintType:
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			hopLimit, hasHopLimit = v, true
			b = b[vn:]
		case num == 12 && typ == protowire.VarintType:
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			// int32 in proto3 is encoded as two's complement uint64 varint
			m.HasRSSI = true
			m.RSSI = int64(int32(v))
			b = b[vn:]
		case num == 15 && typ == protowire.VarintType:
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			hopStart, hasHopStart = v, true
			b = b[vn:]
		default:
			vn := protowire.ConsumeFieldValue(num, typ, b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			b = b[vn:]
		}
	}

	if hasHopStart && hasHopLimit && hopStart > 0 {
		hops := int64(hopStart) - int64(hopLimit)
		if hops < 0 {
			hops = 0
		}
		m.HasHops = true
		m.Hops = hops
	}

	if len(decodedBytes) > 0 {
		return decodeData(decodedBytes, m)
	}
	return nil
}

func decodeData(b []byte, m *TelemetryMetric) error {
	var portnum uint64
	var payload []byte

	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.VarintType:
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			portnum = v
			b = b[vn:]
		case num == 2 && typ == protowire.BytesType:
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			payload = v
			b = b[vn:]
		default:
			vn := protowire.ConsumeFieldValue(num, typ, b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			b = b[vn:]
		}
	}

	m.PacketType = portnumName(portnum)
	if portnum == 67 && len(payload) > 0 {
		return decodeTelemetry(payload, m)
	}
	return nil
}

func decodeTelemetry(b []byte, m *TelemetryMetric) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch {
		case num == 2 && typ == protowire.BytesType:
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			if err := decodeDeviceMetrics(v, m); err != nil {
				return err
			}
			b = b[vn:]
		case num == 3 && typ == protowire.BytesType:
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			if err := decodeEnvMetrics(v, m); err != nil {
				return err
			}
			b = b[vn:]
		default:
			vn := protowire.ConsumeFieldValue(num, typ, b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			b = b[vn:]
		}
	}
	return nil
}

func decodeDeviceMetrics(b []byte, m *TelemetryMetric) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.VarintType:
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			m.HasBattery, m.BatteryLevel = true, uint32(v)
			b = b[vn:]
		case num == 2 && typ == protowire.Fixed32Type:
			v, vn := protowire.ConsumeFixed32(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			m.HasVoltage, m.Voltage = true, float64(math.Float32frombits(v))
			b = b[vn:]
		case num == 3 && typ == protowire.Fixed32Type:
			v, vn := protowire.ConsumeFixed32(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			m.HasChannelUtilization, m.ChannelUtilization = true, float64(math.Float32frombits(v))
			b = b[vn:]
		case num == 4 && typ == protowire.Fixed32Type:
			v, vn := protowire.ConsumeFixed32(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			m.HasAirUtilTx, m.AirUtilTx = true, float64(math.Float32frombits(v))
			b = b[vn:]
		default:
			vn := protowire.ConsumeFieldValue(num, typ, b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			b = b[vn:]
		}
	}
	return nil
}

func decodeEnvMetrics(b []byte, m *TelemetryMetric) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.Fixed32Type:
			v, vn := protowire.ConsumeFixed32(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			m.HasTemperature, m.Temperature = true, float64(math.Float32frombits(v))
			b = b[vn:]
		case num == 2 && typ == protowire.Fixed32Type:
			v, vn := protowire.ConsumeFixed32(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			m.HasHumidity, m.Humidity = true, float64(math.Float32frombits(v))
			b = b[vn:]
		case num == 3 && typ == protowire.Fixed32Type:
			v, vn := protowire.ConsumeFixed32(b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			m.HasPressure, m.Pressure = true, float64(math.Float32frombits(v))
			b = b[vn:]
		default:
			vn := protowire.ConsumeFieldValue(num, typ, b)
			if vn < 0 {
				return protowire.ParseError(vn)
			}
			b = b[vn:]
		}
	}
	return nil
}

func formatNodeID(id uint32) string {
	if id == 0 {
		return ""
	}
	return fmt.Sprintf("!%08x", id)
}

func portnumName(p uint64) string {
	switch p {
	case 1:
		return "text"
	case 3:
		return "position"
	case 4:
		return "nodeinfo"
	case 5:
		return "routing"
	case 6:
		return "admin"
	case 67:
		return "telemetry"
	case 70:
		return "traceroute"
	case 71:
		return "neighborinfo"
	default:
		return fmt.Sprintf("port_%d", p)
	}
}
