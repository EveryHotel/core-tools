package events

import (
	"context"
	"fmt"

	"github.com/rabbitmq/amqp091-go"

	"github.com/EveryHotel/core-tools/pkg/amqp"
)

type amqpDispatcher struct {
	exchanges map[EventName]amqp.AmqpService
	amqpUrl   string
}

type AmqpDispatcher interface {
	AddConsumer(name EventName, consumer amqp.ConsumerHandler, queueSuffix string)
	Dispatch(ctx context.Context, eventName EventName, task amqp.Task) error
	Serve() error
}

func NewAmqpDispatcher(
	amqpUrl string,
) AmqpDispatcher {
	return &amqpDispatcher{
		amqpUrl:   amqpUrl,
		exchanges: make(map[EventName]amqp.AmqpService),
	}
}

func (d *amqpDispatcher) AddConsumer(name EventName, consumer amqp.ConsumerHandler, queueSuffix string) {
	if _, ok := d.exchanges[name]; !ok {
		d.exchanges[name] = amqp.NewAmqpService(d.amqpUrl, string(name), amqp091.ExchangeFanout)
	}

	//для каждого consumer требуется своя очередь
	consumerService := amqp.NewConsumerService(
		consumer,
		fmt.Sprintf("%s_%s", string(name), queueSuffix),
		[]string{string(name)},
		amqp.WithMaxAttempts(3),
		amqp.WithConcurrency(1),
	)

	d.exchanges[name].AddConsumer(consumerService)
}

func (d *amqpDispatcher) Dispatch(
	ctx context.Context,
	eventName EventName,
	task amqp.Task,
) error {
	exchange, ok := d.exchanges[eventName]
	if !ok {
		return fmt.Errorf("exchange %s not found", eventName)
	}

	return exchange.Publish(task, []string{string(eventName)})
}

func (d *amqpDispatcher) Serve() error {
	for _, exchange := range d.exchanges {
		if err := exchange.Serve(); err != nil {
			return err
		}
	}

	return nil
}
