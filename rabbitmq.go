// Copyright (C) 2018 The Nori Authors info@nori.io
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 3 of the License, or (at your option) any later version.
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, see <http://www.gnu.org/licenses/>.
package main

import (
	"context"

	"github.com/cheebo/go-config/types"
	"github.com/cheebo/pubsub"
	"github.com/cheebo/pubsub/rabbitmq"

	"encoding/json"

	cfg "github.com/nori-io/nori-common/config"
	"github.com/nori-io/nori-common/interfaces"
	"github.com/nori-io/nori-common/meta"
	noriPlugin "github.com/nori-io/nori-common/plugin"
)

type plugin struct {
	instance interfaces.PubSub
	pubcfg   *config
	subcfg   *config
}

type pub struct {
	pub pubsub.Publisher
}

type sub struct {
	sub pubsub.Subscriber
}

type msg struct {
	msg pubsub.Message
}

type instance struct {
	pub pub
	sub sub
}

var (
	Plugin plugin
)

type config struct {
	enable       cfg.Bool
	url          cfg.String
	exchange     cfg.String
	queue        cfg.String
	kind         cfg.String
	key          cfg.String
	durable      cfg.Bool
	autoDelete   cfg.Bool
	deliveryMode cfg.UInt
}

func (p *plugin) Init(_ context.Context, configManager cfg.Manager) error {
	m := configManager.Register(p.Meta())
	p.pubcfg = &config{
		enable:       m.Bool("rabbitmq.pub.enable", ""),
		url:          m.String("rabbitmq.pub.url", ""),
		exchange:     m.String("rabbitmq.pub.exchange", ""),
		queue:        m.String("rabbitmq.pub.queue", ""),
		kind:         m.String("rabbitmq.pub.kind", ""),
		key:          m.String("rabbitmq.pub.key", ""),
		durable:      m.Bool("rabbitmq.pub.durable", ""),
		autoDelete:   m.Bool("rabbitmq.pub.auto_delete", ""),
		deliveryMode: m.UInt("rabbitmq.pub.delivery_mode", ""),
	}

	p.subcfg = &config{
		enable:       m.Bool("rabbitmq.sub.enable", ""),
		url:          m.String("rabbitmq.sub.url", ""),
		exchange:     m.String("rabbitmq.sub.exchange", ""),
		queue:        m.String("rabbitmq.sub.queue", ""),
		kind:         m.String("rabbitmq.sub.kind", ""),
		key:          m.String("rabbitmq.sub.key", ""),
		durable:      m.Bool("rabbitmq.sub.durable", ""),
		autoDelete:   m.Bool("rabbitmq.sub.auto_delete", ""),
		deliveryMode: m.UInt("rabbitmq.sub.delivery_mode", ""),
	}
	return nil
}

func (p *plugin) Instance() interface{} {
	return p.instance
}

func (p plugin) Meta() meta.Meta {
	return &meta.Data{
		ID: meta.ID{
			ID:      "nori/pubsub/rabbitmq",
			Version: "1.0",
		},
		Author: meta.Author{
			Name: "Nori",
			URI:  "https://nori.io",
		},
		Core: meta.Core{
			VersionConstraint: ">=1.0, <2.0",
		},
		Dependencies: []meta.Dependency{},
		Description: meta.Description{
			Name: "Nori: RabbitMQ plugin",
		},
		Interface: meta.PubSub,
		License: meta.License{
			Title: "",
			Type:  "GPLv3",
			URI:   "https://www.gnu.org/licenses/"},
		Tags: []string{"queue", "mq", "pubsub", "rabbitmq"},
	}
}

func (p *plugin) Start(ctx context.Context, registry noriPlugin.Registry) error {
	if p.instance == nil {
		var err error
		var pubi pubsub.Publisher
		var subi pubsub.Subscriber
		if p.pubcfg.enable() {
			pubi, err = rabbitmq.NewPublisher(&types.AMQPConfig{
				URL:          p.pubcfg.url(),
				Exchange:     p.pubcfg.exchange(),
				Queue:        p.pubcfg.queue(),
				Kind:         p.pubcfg.kind(),
				Key:          p.pubcfg.key(),
				Durable:      p.pubcfg.durable(),
				AutoDelete:   p.pubcfg.autoDelete(),
				DeliveryMode: p.pubcfg.deliveryMode(),
			}, json.Marshal)
			if err != nil {
				return err
			}
		}

		if p.subcfg.enable() {
			subi, err = rabbitmq.NewSubscriber(&types.AMQPConfig{
				URL:          p.subcfg.url(),
				Exchange:     p.subcfg.exchange(),
				Queue:        p.subcfg.queue(),
				Kind:         p.subcfg.kind(),
				Key:          p.subcfg.key(),
				Durable:      p.subcfg.durable(),
				AutoDelete:   p.subcfg.autoDelete(),
				DeliveryMode: p.subcfg.deliveryMode(),
			}, json.Unmarshal)
			if err != nil {
				return err
			}
		}

		p.instance = &instance{
			pub: pub{
				pub: pubi,
			},
			sub: sub{
				sub: subi,
			},
		}
	}
	return nil
}

func (p *plugin) Stop(_ context.Context, _ noriPlugin.Registry) error {
	var err error
	if p.instance != nil {
		if sub := p.instance.Subscriber(); sub != nil {
			err = sub.Stop()
		}
	}
	p.instance = nil
	p.pubcfg = nil
	p.subcfg = nil
	return err
}

func (i instance) NewPublisher(m interfaces.Marshaller, opts ...func(interface{})) (interfaces.Publisher, error) {
	amqpConfig := &types.AMQPConfig{}
	for _, opt := range opts {
		opt(amqpConfig)
	}
	s, err := rabbitmq.NewPublisher(amqpConfig, func(source interface{}) ([]byte, error) {
		return m(source)
	})
	if err != nil {
		return nil, err
	}

	return &pub{
		pub: s,
	}, nil
}

func (i instance) NewSubscriber(um interfaces.Unmarshaller, opts ...func(interface{})) (interfaces.Subscriber, error) {
	amqpConfig := &types.AMQPConfig{}
	for _, opt := range opts {
		opt(amqpConfig)
	}
	s, err := rabbitmq.NewSubscriber(amqpConfig, func(source []byte, destination interface{}) error {
		return um(source, destination)
	})
	if err != nil {
		return nil, err
	}

	return &sub{
		sub: s,
	}, nil
}

func (i instance) Publisher() interfaces.Publisher {
	return &i.pub
}

func (i instance) Subscriber() interfaces.Subscriber {
	return &i.sub
}

func (p *pub) Publish(ctx context.Context, key string, msg interface{}) error {
	return p.pub.Publish(ctx, key, msg)
}

func (p *pub) Errors() <-chan error {
	return p.pub.Errors()
}

func (s *sub) Start() <-chan interfaces.Message {
	output := make(chan interfaces.Message)
	go func(input <-chan pubsub.Message, output chan interfaces.Message) {
		defer func() {
			close(output)
		}()
		for {
			select {
			case m := <-input:
				output <- &msg{
					msg: m,
				}
			}
		}
	}(s.sub.Start(), output)
	return output
}

func (s *sub) Errors() <-chan error {
	return s.sub.Errors()
}

func (s *sub) Stop() error {
	return s.sub.Stop()
}

func (m *msg) UnMarshal(msg interface{}) error {
	return m.msg.UnMarshal(msg)
}

func (m *msg) Done() error {
	return m.msg.Done()
}
