package amqp

import (
	"encoding/json"

	"github.com/wagslane/go-rabbitmq"
)

type AmqpService interface {
	AddConsumer(ConsumerService)
	Publish(Task, []string) error
	Serve() error
	Close() error
}

type amqpService struct {
	amqpUrl           string
	exchangeName      string
	exchangeType      string
	consumers         []ConsumerService
	connection        *rabbitmq.Conn
	internalConsumers []*rabbitmq.Consumer
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
func (s *amqpService) Publish(task Task, routingKeys []string) error {
	conn, err := rabbitmq.NewConn(
		s.amqpUrl,
		rabbitmq.WithConnectionOptionsLogging,
	)
	if err != nil {
		return err
	}

	defer conn.Close()

	publisher, err := rabbitmq.NewPublisher(conn,
		rabbitmq.WithPublisherOptionsLogging,
		rabbitmq.WithPublisherOptionsExchangeName(s.exchangeName),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
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
	var err error
	if s.connection == nil {
		s.connection, err = rabbitmq.NewConn(
			s.amqpUrl,
			rabbitmq.WithConnectionOptionsLogging,
		)
		if err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}

	handler := func(message rabbitmq.Delivery) rabbitmq.Action {
		return s.handleMessage(message, consumerService)
	}

	opts := []func(options *rabbitmq.ConsumerOptions){
		rabbitmq.WithConsumerOptionsExchangeName(s.exchangeName),
		rabbitmq.WithConsumerOptionsExchangeKind(s.exchangeType),
		rabbitmq.WithConsumerOptionsConsumerAutoAck(false),
		rabbitmq.WithConsumerOptionsExchangeDeclare,
		rabbitmq.WithConsumerOptionsQueueDurable,
		rabbitmq.WithConsumerOptionsExchangeDurable,
		rabbitmq.WithConsumerOptionsConcurrency(consumerService.GetConcurrency()),
	}

	for _, routing := range consumerService.GetRoutingKeys() {
		opts = append(opts, rabbitmq.WithConsumerOptionsRoutingKey(routing))
	}

	consumer, err := rabbitmq.NewConsumer(s.connection,
		handler,
		consumerService.GetQueueName(),
		opts...,
	)

	if err != nil {
		return err
	}

	s.internalConsumers = append(s.internalConsumers, consumer)
	return nil
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

		if task.GetFailedCallback() != nil {
			task.GetFailedCallback()()
		}
		return rabbitmq.NackDiscard
	}

	return res
}

func (s *amqpService) Close() error {
	for _, consumer := range s.internalConsumers {
		consumer.Close()
	}

	if s.connection != nil {
		err := s.connection.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
