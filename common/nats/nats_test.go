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
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	gnatsd "github.com/nats-io/gnatsd/test"
	nats "github.com/nats-io/go-nats"
	"github.com/stretchr/testify/assert"
	"k8s.io/heapster/metrics/core"

	"github.com/nats-io/gnatsd/server"
)

type NatsSinkPoint struct {
	MetricsName  string
	MetricsValue map[string]core.MetricValue
	MetricsTags  map[string]string
}

func runDefaultServer() *server.Server {
	opts := gnatsd.DefaultTestOptions
	opts.Port = nats.DefaultPort
	return gnatsd.RunServer(&opts)
}

func newJsonEncodedConn(t *testing.T) *nats.EncodedConn {
	url := fmt.Sprintf("nats://localhost:%d", nats.DefaultPort)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Failed to create default connection: %v\n", err)
		return nil
	}
	ec, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		t.Fatalf("Failed to create default connection: %v\n", err)
		return nil
	}
	return ec
}

func waitTime(ch chan bool, timeout time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
	}
	return errors.New("timeout")
}

func TestNatsClientWrite(t *testing.T) {
	ds := runDefaultServer()
	defer ds.Shutdown()

	tags := make(map[string]string)
	tags["container_id"] = "aaaa"
	sinkPoint := &NatsSinkPoint{
		MetricsName: "test",
		MetricsValue: map[string]core.MetricValue{
			"/system.slice/-.mount//cpu/limit": {
				ValueType:  core.ValueInt64,
				MetricType: core.MetricCumulative,
				IntValue:   123456,
			},
		},
		MetricsTags: tags,
	}

	ec := newJsonEncodedConn(t)
	defer ec.Close()
	ch := make(chan bool)
	defer close(ch)
	_, err := ec.Subscribe("heapster-connect", func(point *NatsSinkPoint) {
		assert.Equal(t, sinkPoint, point)
		ch <- true
	})
	assert.NoError(t, err)

	url, err := url.Parse("nats:?brokers=" + nats.DefaultURL + "&subject=heapster-connect")
	assert.NoError(t, err)
	client, err := NewNatsClient(url)
	assert.NoError(t, err)
	defer client.Stop()

	err = client.ProduceMessage(sinkPoint)
	assert.NoError(t, err)

	if e := waitTime(ch, 5*time.Second); e != nil {
		t.Fatal("Did not receive the message")
	}
}
