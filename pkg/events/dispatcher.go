package events

import (
	"context"
	"fmt"
)

type EventName string

type Listener func(ctx context.Context, event interface{}) error

type Subscriber map[EventName][]Listener

type Dispatcher interface {
	AddListener(EventName, Listener)
	AddSubscriber(Subscriber)
	Dispatch(context.Context, EventName, interface{}) error
}

type dispatcher struct {
	events map[EventName][]Listener
}

func NewDispatcher() Dispatcher {
	return &dispatcher{
		events: make(map[EventName][]Listener),
	}
}

func (d *dispatcher) AddListener(name EventName, listener Listener) {
	d.events[name] = append(d.events[name], listener)
}

func (d *dispatcher) AddSubscriber(subscriber Subscriber) {
	for eventName, listeners := range subscriber {
		for _, listener := range listeners {
			d.AddListener(eventName, listener)
		}
	}
}

func (d *dispatcher) Dispatch(ctx context.Context, name EventName, event interface{}) error {
	for i, listener := range d.events[name] {
		if err := listener(ctx, event); err != nil {
			return fmt.Errorf("dispatch event %s with %d listener: %w", name, i, err)
		}
	}

	return nil
}
