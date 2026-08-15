// Package config defines the versioned configuration consumed by the pacer
// service. Configuration is supplied as JSON (see examples/config.json) and is
// trusted input: it is loaded once at startup by an operator, so its
// validation focuses on structural consistency rather than the strict protocol
// checks applied to untrusted OpenRTB requests.
//
// A configuration pins a Version; the persistence layer records this version in
// every checkpoint and refuses to start when the on-disk state was written
// under a different version, preventing silent schema drift.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// MaxIDLen is the shared upper bound on identifier length.
const MaxIDLen = 128

// Config is the top-level service configuration.
type Config struct {
	// Version is the configuration schema/version. Changing it invalidates
	// existing checkpoints.
	Version int64 `json:"version"`
	// ListenAddr is the HTTP listen address.
	ListenAddr string `json:"listen_addr"`
	// ShardCount is the number of campaign-shard workers.
	ShardCount int `json:"shard_count"`
	// QueueCap is the per-shard queue capacity. When full, new reservations
	// for that shard are rejected with queue_full.
	QueueCap int `json:"queue_cap"`
	// ReservationTTL is how long a reservation stays RESERVED before it is
	// eligible for expiry.
	ReservationTTL Duration `json:"reservation_ttl"`
	// CheckpointThreshold is the number of committed operations between
	// automatic checkpoints. A value <= 0 disables automatic checkpointing.
	CheckpointThreshold int `json:"checkpoint_threshold"`
	// Campaigns is the set of configured campaigns.
	Campaigns []Campaign `json:"campaigns"`
}

// Campaign is a single advertiser campaign with its three-tier budget tree.
type Campaign struct {
	ID          string   `json:"id"`
	LimitMicros int64    `json:"limit_micros"`
	BurstMicros int64    `json:"burst_micros"`
	Periods     []Period `json:"periods"`
	Channels    []Channel `json:"channels"`
}

// Period is a time-bounded delivery window within a campaign.
type Period struct {
	ID          string    `json:"id"`
	Start       time.Time `json:"start"`
	End         time.Time `json:"end"`
	LimitMicros int64     `json:"limit_micros"`
}

// Channel is a delivery channel within a campaign's period.
type Channel struct {
	ID          string `json:"id"`
	LimitMicros int64  `json:"limit_micros"`
}

// Duration is a time.Duration that marshals to/from JSON as a string such as
// "30s" or "5m". It keeps configuration human-readable while preserving
// nanosecond precision.
type Duration time.Duration

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// Load reads and validates a configuration file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate checks structural consistency of the configuration.
func (c *Config) Validate() error {
	if c.Version <= 0 {
		return fmt.Errorf("config: version must be positive")
	}
	if c.ShardCount <= 0 {
		return fmt.Errorf("config: shard_count must be positive")
	}
	if c.QueueCap <= 0 {
		return fmt.Errorf("config: queue_cap must be positive")
	}
	if c.ReservationTTL <= 0 {
		return fmt.Errorf("config: reservation_ttl must be positive")
	}
	if len(c.Campaigns) == 0 {
		return fmt.Errorf("config: at least one campaign required")
	}
	seenCampaign := make(map[string]struct{}, len(c.Campaigns))
	for i := range c.Campaigns {
		cm := &c.Campaigns[i]
		if err := validateID(cm.ID, "campaign"); err != nil {
			return err
		}
		if _, dup := seenCampaign[cm.ID]; dup {
			return fmt.Errorf("config: duplicate campaign %q", cm.ID)
		}
		seenCampaign[cm.ID] = struct{}{}
		if cm.LimitMicros <= 0 {
			return fmt.Errorf("config: campaign %q limit must be positive", cm.ID)
		}
		if cm.BurstMicros < 0 {
			return fmt.Errorf("config: campaign %q burst must be non-negative", cm.ID)
		}
		if cm.BurstMicros > cm.LimitMicros {
			return fmt.Errorf("config: campaign %q burst exceeds limit", cm.ID)
		}
		if len(cm.Periods) == 0 {
			return fmt.Errorf("config: campaign %q has no periods", cm.ID)
		}
		if len(cm.Channels) == 0 {
			return fmt.Errorf("config: campaign %q has no channels", cm.ID)
		}
		seenPeriod := make(map[string]struct{}, len(cm.Periods))
		var prevEnd time.Time
		for j := range cm.Periods {
			p := &cm.Periods[j]
			if err := validateID(p.ID, "period"); err != nil {
				return err
			}
			if _, dup := seenPeriod[p.ID]; dup {
				return fmt.Errorf("config: campaign %q duplicate period %q", cm.ID, p.ID)
			}
			seenPeriod[p.ID] = struct{}{}
			if !p.End.After(p.Start) {
				return fmt.Errorf("config: campaign %q period %q end must be after start", cm.ID, p.ID)
			}
			if !prevEnd.IsZero() && p.Start.Before(prevEnd) {
				return fmt.Errorf("config: campaign %q periods overlap", cm.ID)
			}
			prevEnd = p.End
			if p.LimitMicros <= 0 {
				return fmt.Errorf("config: campaign %q period %q limit must be positive", cm.ID, p.ID)
			}
			if p.LimitMicros > cm.LimitMicros {
				return fmt.Errorf("config: campaign %q period %q limit exceeds campaign limit", cm.ID, p.ID)
			}
		}
		seenChannel := make(map[string]struct{}, len(cm.Channels))
		for j := range cm.Channels {
			ch := &cm.Channels[j]
			if err := validateID(ch.ID, "channel"); err != nil {
				return err
			}
			if _, dup := seenChannel[ch.ID]; dup {
				return fmt.Errorf("config: campaign %q duplicate channel %q", cm.ID, ch.ID)
			}
			seenChannel[ch.ID] = struct{}{}
			if ch.LimitMicros <= 0 {
				return fmt.Errorf("config: campaign %q channel %q limit must be positive", cm.ID, ch.ID)
			}
			if ch.LimitMicros > cm.LimitMicros {
				return fmt.Errorf("config: campaign %q channel %q limit exceeds campaign limit", cm.ID, ch.ID)
			}
		}
	}
	return nil
}

func validateID(s, kind string) error {
	if s == "" {
		return fmt.Errorf("config: %s id is empty", kind)
	}
	if len(s) > MaxIDLen {
		return fmt.Errorf("config: %s id too long", kind)
	}
	return nil
}

// CampaignByID returns the campaign with the given id and ok=false if absent.
func (c *Config) CampaignByID(id string) (Campaign, bool) {
	for i := range c.Campaigns {
		if c.Campaigns[i].ID == id {
			return c.Campaigns[i], true
		}
	}
	return Campaign{}, false
}

// PeriodAt returns the period of campaign cm that contains t (start <= t < end),
// or ok=false if none.
func (cm *Campaign) PeriodAt(t time.Time) (Period, bool) {
	for i := range cm.Periods {
		p := &cm.Periods[i]
		if !t.Before(p.Start) && t.Before(p.End) {
			return *p, true
		}
	}
	return Period{}, false
}

// ChannelByID returns the channel with the given id.
func (cm *Campaign) ChannelByID(id string) (Channel, bool) {
	for i := range cm.Channels {
		if cm.Channels[i].ID == id {
			return cm.Channels[i], true
		}
	}
	return Channel{}, false
}
