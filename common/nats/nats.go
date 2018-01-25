// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nats

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/url"

	"github.com/golang/glog"
	nats "github.com/nats-io/go-nats"
)

type NatsClient interface {
	Name() string
	Stop()
	ProduceMessage(msgData interface{}) error
}

type natsConfig struct {
	user     string
	password string
	subject  string
	servers  []string
	cacert   string
	cert     string
	key      string
	token    string
}

type natsSink struct {
	subject string
	conn    *nats.EncodedConn
}

func (sink *natsSink) Name() string {
	return "Nats Sink"
}

func (sink *natsSink) Stop() {
	sink.conn.Close()
}

func (sink *natsSink) ProduceMessage(msgData interface{}) error {
	return sink.conn.Publish(sink.subject, msgData)
}

func newNatsConnection(config *natsConfig) (*nats.EncodedConn, error) {
	options := nats.GetDefaultOptions()
	if config.cacert != "" {
		caCert, err := ioutil.ReadFile(config.cacert)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig := &tls.Config{
			RootCAs: caCertPool,
		}
		if config.cert != "" && config.key != "" {
			cert, err := tls.LoadX509KeyPair(config.cert, config.key)
			if err != nil {
				return nil, err
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		options.TLSConfig = tlsConfig
		options.Secure = true
	}
	options.Servers = config.servers
	options.User = config.user
	options.Password = config.password
	options.Token = config.token

	glog.V(6).Infof("use options %+v", options)
	c, err := options.Connect()
	if err != nil {
		return nil, err
	}
	glog.V(6).Infof("connection established %+v", c)
	return nats.NewEncodedConn(c, nats.JSON_ENCODER)
}

func NewNatsClient(uri *url.URL) (NatsClient, error) {
	sink := &natsSink{}
	opts, err := url.ParseQuery(uri.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url's query string: %s", err)
	}
	glog.V(6).Infof("nats sink option: %v", opts)

	config := natsConfig{
		servers: []string{nats.DefaultURL},
		subject: "heapster.subject",
	}

	if len(opts["brokers"]) > 0 {
		config.servers = opts["brokers"]
	}
	if len(opts["user"]) > 0 {
		config.user = opts["user"][0]
	}
	if len(opts["password"]) > 0 {
		config.password = opts["password"][0]
	}
	if len(opts["token"]) > 0 {
		config.token = opts["token"][0]
	}
	if len(opts["cacert"]) > 0 {
		config.cacert = opts["cacert"][0]
	}
	if len(opts["cert"]) > 0 {
		config.cert = opts["cert"][0]
	}
	if len(opts["key"]) > 0 {
		config.key = opts["key"][0]
	}
	if len(opts["subject"]) > 0 {
		sink.subject = opts["subject"][0]
	}

	glog.V(3).Infof("attempt to connect to nats brokers")
	sink.conn, err = newNatsConnection(&config)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to nats brokers: - %s", err)
	}
	glog.V(3).Infof("nats sink connection fully operational")
	return sink, nil
}
