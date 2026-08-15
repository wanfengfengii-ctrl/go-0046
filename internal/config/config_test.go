package config

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func mustMarshalJSON(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func jsonUnmarshalStrict(data []byte, v interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func writeFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o644)
}

func validConfig() *Config {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &Config{
		Version:             1,
		ListenAddr:          ":8080",
		ShardCount:          4,
		QueueCap:            16,
		ReservationTTL:      Duration(30 * time.Second),
		CheckpointThreshold: 50,
		Campaigns: []Campaign{{
			ID:          "camp-1",
			LimitMicros: 1_000_000,
			BurstMicros: 100_000,
			Periods: []Period{{
				ID:          "p1",
				Start:       t0,
				End:         t0.Add(time.Hour),
				LimitMicros: 500_000,
			}},
			Channels: []Channel{{
				ID:          "ch-1",
				LimitMicros: 200_000,
			}},
		}},
	}
}

func TestConfigValidateOK(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfigValidateRejects(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Config)
	}{
		{"zero version", func(c *Config) { c.Version = 0 }},
		{"zero shards", func(c *Config) { c.ShardCount = 0 }},
		{"zero queue", func(c *Config) { c.QueueCap = 0 }},
		{"zero ttl", func(c *Config) { c.ReservationTTL = 0 }},
		{"no campaigns", func(c *Config) { c.Campaigns = nil }},
		{"campaign limit zero", func(c *Config) { c.Campaigns[0].LimitMicros = 0 }},
		{"burst over limit", func(c *Config) { c.Campaigns[0].BurstMicros = 2_000_000 }},
		{"no periods", func(c *Config) { c.Campaigns[0].Periods = nil }},
		{"no channels", func(c *Config) { c.Campaigns[0].Channels = nil }},
		{"period end before start", func(c *Config) {
			c.Campaigns[0].Periods[0].End = c.Campaigns[0].Periods[0].Start
		}},
		{"period limit over campaign", func(c *Config) {
			c.Campaigns[0].Periods[0].LimitMicros = 9_000_000
		}},
		{"channel limit over campaign", func(c *Config) {
			c.Campaigns[0].Channels[0].LimitMicros = 9_000_000
		}},
		{"duplicate period", func(c *Config) {
			c.Campaigns[0].Periods = append(c.Campaigns[0].Periods, c.Campaigns[0].Periods[0])
		}},
		{"duplicate channel", func(c *Config) {
			c.Campaigns[0].Channels = append(c.Campaigns[0].Channels, c.Campaigns[0].Channels[0])
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.mut(c)
			if err := c.Validate(); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestConfigPeriodAt(t *testing.T) {
	c := validConfig()
	cm, _ := c.CampaignByID("camp-1")
	start := cm.Periods[0].Start
	if _, ok := cm.PeriodAt(start); !ok {
		t.Fatal("period should contain its own start")
	}
	if _, ok := cm.PeriodAt(start.Add(time.Hour)); ok {
		t.Fatal("period should not contain its own end")
	}
	if _, ok := cm.PeriodAt(start.Add(30 * time.Minute)); !ok {
		t.Fatal("midpoint should be in period")
	}
}

func TestConfigDurationRoundTrip(t *testing.T) {
	t.Parallel()
	c := validConfig()
	data := mustMarshalJSON(t, c)
	var got Config
	if err := jsonUnmarshalStrict(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ReservationTTL != c.ReservationTTL {
		t.Fatalf("ttl = %v, want %v", got.ReservationTTL, c.ReservationTTL)
	}
}

func TestConfigLoadFile(t *testing.T) {
	c := validConfig()
	data := mustMarshalJSON(t, c)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := writeFile(path, data); err != nil {
		t.Fatalf("write: %v", err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.Version != c.Version || loaded.ShardCount != c.ShardCount {
		t.Fatalf("loaded mismatch: %+v", loaded)
	}
}

func TestConfigLoadRejectsUnknownField(t *testing.T) {
	data := append(mustMarshalJSON(t, validConfig()), []byte(`, "unknown": 1}`)...)
	// Replace the trailing } from validConfig() with a comma so the appended
	// object is well-formed. Simpler: craft a JSON with an unknown field.
	data = []byte(`{"version":1,"listen_addr":":8080","shard_count":4,"queue_cap":16,"reservation_ttl":"30s","checkpoint_threshold":50,"campaigns":[{"id":"c","limit_micros":1000,"burst_micros":0,"periods":[{"id":"p","start":"2026-01-01T00:00:00Z","end":"2026-01-01T01:00:00Z","limit_micros":500}],"channels":[{"id":"ch","limit_micros":500}]}],"unknown_field":1}`)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := writeFile(path, data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected unknown-field error")
	}
}
