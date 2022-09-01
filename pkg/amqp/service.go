package amqp

import (
	"encoding/json"

	"github.com/wagslane/go-rabbitmq"
)

type AmqpService interface {
	AddConsumer(ConsumerService)
	Publish(Task, []string) error
	Serve() error
}

type amqpService struct {
	amqpUrl      string
	exchangeName string
	exchangeType string
	consumers    []ConsumerService
}

func NewAmqpService(amqpUrl string, exchangeName, exchangeType string) AmqpService {
	return &amqpService{
		amqpUrl:      amqpUrl,
		exchangeName: exchangeName,
		exchangeType: exchangeType,
	}
}

// AddConsumer добавляет новый обработчик сообщений
func (s *amqpService) AddConsumer(consumer ConsumerService) {
	s.consumers = append(s.consumers, consumer)
}

// Publish публикует новую задачу в пулл по заданным routingKeys
func (s amqpService) Publish(task Task, routingKeys []string) error {
	publisher, err := rabbitmq.NewPublisher(s.amqpUrl,
		rabbitmq.Config{},
	)
	if err != nil {
		return err
	}
	defer publisher.Close()

	task.IncrAttemptNumber()
	message, err := json.Marshal(task)
	if err != nil {
		return err
	}

	err = publisher.Publish(
		message,
		routingKeys,
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsMandatory,
		rabbitmq.WithPublishOptionsPersistentDelivery,
		rabbitmq.WithPublishOptionsExchange(s.exchangeName),
	)
	if err != nil {
		return err
	}

	return nil
}

// Serve запускает в работу всех заранее добавленных обработчиков
func (s *amqpService) Serve() error {
	for _, consumer := range s.consumers {
		if err := s.runConsumer(consumer); err != nil {
			return err
		}
	}

	return nil
}

// runConsumer запускает обработчика задач в работу
func (s *amqpService) runConsumer(consumerService ConsumerService) error {
	consumer, err := rabbitmq.NewConsumer(s.amqpUrl,
		rabbitmq.Config{},
		rabbitmq.WithConsumerOptionsLogging,
	)
	if err != nil {
		return err
	}

	handler := func(message rabbitmq.Delivery) rabbitmq.Action {
		return s.handleMessage(message, consumerService)
	}

	err = consumer.StartConsuming(handler,
		consumerService.GetQueueName(),
		consumerService.GetRoutingKeys(),
		rabbitmq.WithConsumeOptionsBindingExchangeName(s.exchangeName),
		rabbitmq.WithConsumeOptionsBindingExchangeKind(s.exchangeType),
		rabbitmq.WithConsumeOptionsConsumerAutoAck(false),
		rabbitmq.WithConsumeOptionsQueueDurable,
		rabbitmq.WithConsumeOptionsBindingExchangeDurable,
		rabbitmq.WithConsumeOptionsConcurrency(consumerService.GetConcurrency()),
	)

	return err
}

// handleMessage функция обертка для обработки сообщений
func (s *amqpService) handleMessage(message rabbitmq.Delivery, consumer ConsumerService) rabbitmq.Action {
	task, res := consumer.GetHandler().Handle(message)

	if res == rabbitmq.NackRequeue {
		newAttempt := task.IncrAttemptNumber()
		if newAttempt < consumer.GetMaxAttempts() {
			if err := s.Publish(task, consumer.GetRoutingKeys()); err != nil {
				return rabbitmq.NackDiscard
			}
		}

		task.GetFailedCallback()()
		return rabbitmq.NackDiscard
	}

	return res
}
