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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube_api "k8s.io/client-go/pkg/api/v1"
	event_core "k8s.io/heapster/events/core"
)

type fakeNatsClient struct {
	points []NatsSinkPoint
}

type fakeNatsSink struct {
	event_core.EventSink
	fakeClient *fakeNatsClient
}

func NewFakeNatsClient() *fakeNatsClient {
	return &fakeNatsClient{[]NatsSinkPoint{}}
}

func (client *fakeNatsClient) ProduceMessage(msgData interface{}) error {
	if point, ok := msgData.(NatsSinkPoint); ok {
		client.points = append(client.points, point)
	}

	return nil
}

func (client *fakeNatsClient) Name() string {
	return "Apache Nats Sink"
}

func (client *fakeNatsClient) Stop() {
	// nothing needs to be done.
}

// Returns a fake nats sink.
func NewFakeSink() fakeNatsSink {
	client := NewFakeNatsClient()
	return fakeNatsSink{
		&natsSink{
			NatsClient: client,
		},
		client,
	}
}

func TestStoreDataEmptyInput(t *testing.T) {
	fakeSink := NewFakeSink()
	eventsBatch := event_core.EventBatch{}
	fakeSink.ExportEvents(&eventsBatch)
	assert.Equal(t, 0, len(fakeSink.fakeClient.points))
}

func TestStoreMultipleEventsInput(t *testing.T) {
	fakeSink := NewFakeSink()
	timestamp := time.Now()
	now := time.Now()
	event1 := kube_api.Event{
		Message:        "event1",
		Count:          100,
		LastTimestamp:  metav1.NewTime(now),
		FirstTimestamp: metav1.NewTime(now),
	}
	event2 := kube_api.Event{
		Message:        "event2",
		Count:          101,
		LastTimestamp:  metav1.NewTime(now),
		FirstTimestamp: metav1.NewTime(now),
	}
	data := event_core.EventBatch{
		Timestamp: timestamp,
		Events: []*kube_api.Event{
			&event1,
			&event2,
		},
	}
	fakeSink.ExportEvents(&data)
	// expect msg string
	assert.Equal(t, 2, len(fakeSink.fakeClient.points))

}
