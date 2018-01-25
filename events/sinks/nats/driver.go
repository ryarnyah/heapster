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
	"encoding/json"
	"net/url"
	"sync"
	"time"

	"github.com/golang/glog"
	kube_api "k8s.io/client-go/pkg/api/v1"
	nats_common "k8s.io/heapster/common/nats"
	event_core "k8s.io/heapster/events/core"
	"k8s.io/heapster/metrics/core"
)

type NatsSinkPoint struct {
	EventValue     interface{}
	EventTimestamp time.Time
	EventTags      map[string]string
}

type natsSink struct {
	nats_common.NatsClient
	sync.RWMutex
}

func getEventValue(event *kube_api.Event) (string, error) {
	// TODO: check whether indenting is required.
	bytes, err := json.MarshalIndent(event, "", " ")
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func eventToPoint(event *kube_api.Event) (*NatsSinkPoint, error) {
	value, err := getEventValue(event)
	if err != nil {
		return nil, err
	}
	point := NatsSinkPoint{
		EventTimestamp: event.LastTimestamp.Time.UTC(),
		EventValue:     value,
		EventTags: map[string]string{
			"eventID": string(event.UID),
		},
	}
	if event.InvolvedObject.Kind == "Pod" {
		point.EventTags[core.LabelPodId.Key] = string(event.InvolvedObject.UID)
		point.EventTags[core.LabelPodName.Key] = event.InvolvedObject.Name
	}
	point.EventTags[core.LabelHostname.Key] = event.Source.Host
	return &point, nil
}

func (sink *natsSink) ExportEvents(eventBatch *event_core.EventBatch) {
	sink.Lock()
	defer sink.Unlock()

	for _, event := range eventBatch.Events {
		point, err := eventToPoint(event)
		if err != nil {
			glog.Warningf("Failed to convert event to point: %v", err)
		}

		err = sink.ProduceMessage(*point)
		if err != nil {
			glog.Errorf("Failed to produce event message: %s", err)
		}
	}
}

func NewNatsSink(uri *url.URL) (event_core.EventSink, error) {
	client, err := nats_common.NewNatsClient(uri)
	if err != nil {
		return nil, err
	}

	return &natsSink{
		NatsClient: client,
	}, nil
}
