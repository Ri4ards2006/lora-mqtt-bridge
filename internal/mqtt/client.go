package mqtt

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// MessageHandler is invoked for each received MQTT message.
type MessageHandler func(topic string, payload []byte, receivedAt time.Time)

// ClientConfig holds MQTT connection and subscription settings.
type ClientConfig struct {
	Broker   string
	Topic    string
	ClientID string
	Username string
	Password string
	QoS      byte
}

// Client wraps paho.mqtt.golang with auto-reconnect, automatic resubscription,
// and atomic connection readiness tracking.
type Client struct {
	cfg       ClientConfig
	client    paho.Client
	handler   MessageHandler
	logger    *slog.Logger
	connected atomic.Bool
}

// NewClient constructs a resilient MQTT client.
func NewClient(cfg ClientConfig, handler MessageHandler, logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	c := &Client{
		cfg:     cfg,
		handler: handler,
		logger:  logger,
	}

	opts := paho.NewClientOptions().
		AddBroker(cfg.Broker).
		SetClientID(cfg.ClientID).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(3 * time.Second).
		SetMaxReconnectInterval(30 * time.Second).
		SetKeepAlive(30 * time.Second).
		SetPingTimeout(10 * time.Second).
		SetCleanSession(true).
		SetOrderMatters(false)

	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		opts.SetPassword(cfg.Password)
	}

	opts.SetOnConnectHandler(func(pc paho.Client) {
		c.connected.Store(true)
		c.logger.Info("mqtt connected, subscribing", "broker", cfg.Broker, "topic", cfg.Topic)
		tok := pc.Subscribe(cfg.Topic, cfg.QoS, func(_ paho.Client, msg paho.Message) {
			// Copy payload slice so workers never race with paho internal buffers.
			raw := make([]byte, len(msg.Payload()))
			copy(raw, msg.Payload())
			c.handler(msg.Topic(), raw, time.Now().UTC())
		})
		if tok.WaitTimeout(10*time.Second) && tok.Error() != nil {
			c.logger.Error("mqtt subscribe failed", "topic", cfg.Topic, "err", tok.Error())
		}
	})

	opts.SetConnectionLostHandler(func(_ paho.Client, err error) {
		c.connected.Store(false)
		c.logger.Warn("mqtt connection lost, will auto-reconnect", "err", err)
	})

	c.client = paho.NewClient(opts)
	return c
}

// Connect initiates connection to the broker, respecting ctx cancellation.
func (c *Client) Connect(ctx context.Context) error {
	tok := c.client.Connect()
	done := make(chan struct{})
	go func() {
		tok.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		if err := tok.Error(); err != nil {
			return fmt.Errorf("mqtt connect %s: %w", c.cfg.Broker, err)
		}
		return nil
	}
}

// IsConnected reports whether the MQTT client currently has an active session.
func (c *Client) IsConnected() bool {
	return c.connected.Load() && c.client.IsConnected()
}

// Close gracefully unsubscribes and disconnects from the broker.
func (c *Client) Close() {
	if c.client.IsConnected() {
		unsub := c.client.Unsubscribe(c.cfg.Topic)
		unsub.WaitTimeout(2 * time.Second)
	}
	c.connected.Store(false)
	c.client.Disconnect(1000)
}
