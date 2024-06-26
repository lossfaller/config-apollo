package model

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/bytedance/gopkg/lang/fastrand"
	"github.com/cloudwego/configmanager/iface"
	"github.com/cloudwego/kitex/pkg/acl"
)

var errRejected = errors.New("rejected by client degradation config")

var defaultConfig = &Config{
	Enable:     false,
	Percentage: 0,
}

type Config struct {
	Enable     bool `json:"enable"`
	Percentage int  `json:"percentage"`
}

// DeepCopy returns a copy of the current Config
func (c *Config) DeepCopy() iface.ConfigValueItem {
	// Check if the receiver is nil
	if c == nil {
		return nil
	}

	result := &Config{
		Enable:     c.Enable,
		Percentage: c.Percentage,
	}
	return result
}

// EqualsTo returns true if the current Config equals to the other Config
func (c *Config) EqualsTo(other iface.ConfigValueItem) bool {
	o := other.(*Config)
	return c.Enable == o.Enable && c.Percentage == o.Percentage
}

// Container is a wrapper for Config
type Container struct {
	config atomic.Value
}

func NewContainer() *Container {
	c := &Container{}
	c.config.Store(defaultConfig)
	return c
}

// NotifyPolicyChange to receive policy when it changes
func (c *Container) NotifyPolicyChange(cfg *Config) {
	c.config.Store(cfg)
}

func (c *Container) GetAclRule() acl.RejectFunc {
	return func(ctx context.Context, request interface{}) (reason error) {
		cfg := c.config.Load().(*Config)
		if !cfg.Enable {
			return nil
		}
		if fastrand.Intn(100) < cfg.Percentage {
			return errRejected
		}
		return nil
	}
}
