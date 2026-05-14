package amqp

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq"
)

// TODO everyHotel
//  Добавлены новые методы
//  Есть различия в реализации методов Publish и runConsumer
//  В момент перехода на эту либу надо это все проверить.

type AmqpService interface {
	AddConsumer(ConsumerService)
	Publish(Task, []string) error
	Serve() error
	Close() error
	SetLogLevel(slog.Level)
}

type amqpService struct {
	amqpUrl           string
	exchangeName      string
	exchangeType      string
	consumers         []ConsumerService
	connMu            sync.Mutex
	connection        *rabbitmq.Conn
	pubMu             sync.Mutex
	publisher         *rabbitmq.Publisher
	internalConsumers []*rabbitmq.Consumer
	logLevel          slog.Level
}

func NewAmqpService(amqpUrl string, exchangeName, exchangeType string) AmqpService {
	return &amqpService{
		amqpUrl:      amqpUrl,
		exchangeName: exchangeName,
		exchangeType: exchangeType,
		logLevel:     slog.LevelWarn,
	}
}

func (s *amqpService) getConnection(logMsg string) (*rabbitmq.Conn, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	if s.connection != nil {
		return s.connection, nil
	}

	conn, err := rabbitmq.NewConn(
		s.amqpUrl,
		rabbitmq.WithConnectionOptionsLogger(NewLogger(s.logLevel, logMsg)),
	)
	if err != nil {
		return nil, err
	}

	s.connection = conn
	return s.connection, nil
}

func (s *amqpService) resetConnection() {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	s.pubMu.Lock()
	if s.publisher != nil {
		s.publisher.Close()
		s.publisher = nil
	}
	s.pubMu.Unlock()

	if s.connection != nil {
		_ = s.connection.Close()
		s.connection = nil
	}
}

func (s *amqpService) getPublisher(logMsg string) (*rabbitmq.Publisher, error) {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()

	if s.publisher != nil {
		return s.publisher, nil
	}

	conn, err := s.getConnection(logMsg)
	if err != nil {
		return nil, err
	}

	publisher, err := rabbitmq.NewPublisher(conn,
		rabbitmq.WithPublisherOptionsExchangeName(s.exchangeName),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
		rabbitmq.WithPublisherOptionsExchangeKind(s.exchangeType),
		rabbitmq.WithPublisherOptionsExchangeDurable,
		rabbitmq.WithPublisherOptionsLogger(
			NewLogger(
				s.logLevel,
				fmt.Sprintf("publisher exchange: %s", s.exchangeName),
			)),
	)
	if err != nil {
		return nil, err
	}

	s.publisher = publisher
	return s.publisher, nil
}

func isConnNotOpenErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return errors.Is(err, amqp091.ErrClosed) ||
		strings.Contains(msg, "Exception (504)") ||
		strings.Contains(msg, "reconnecting to amqp server")
}

func (s *amqpService) SetLogLevel(level slog.Level) {
	s.logLevel = level
}

// AddConsumer добавляет новый обработчик сообщений
func (s *amqpService) AddConsumer(consumer ConsumerService) {
	s.consumers = append(s.consumers, consumer)
}

// Publish публикует новую задачу в пулл по заданным routingKeys
func (s *amqpService) Publish(task Task, routingKeys []string) error {
	publishOnce := func() error {
		publisher, err := s.getPublisher(
			fmt.Sprintf(
				"connection for publish exchange: %s routing: %s",
				s.exchangeName,
				strings.Join(routingKeys, ", "),
			),
		)
		if err != nil {
			return err
		}

		message, err := json.Marshal(task)
		if err != nil {
			return err
		}

		return publisher.Publish(
			message,
			routingKeys,
			rabbitmq.WithPublishOptionsContentType("application/json"),
			rabbitmq.WithPublishOptionsMandatory,
			rabbitmq.WithPublishOptionsPersistentDelivery,
			rabbitmq.WithPublishOptionsExchange(s.exchangeName),
		)
	}

	// 1-я попытка
	if err := publishOnce(); err != nil {
		if isConnNotOpenErr(err) {
			s.resetConnection()
			return publishOnce()
		}
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
	conn, err := s.getConnection(
		fmt.Sprintf(
			"connection for consumer exchange: %s queue: %s routing: %s",
			s.exchangeName,
			consumerService.GetQueueName(),
			strings.Join(consumerService.GetRoutingKeys(), ", "),
		),
	)
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
		rabbitmq.WithConsumerOptionsLogger(
			NewLogger(
				s.logLevel,
				fmt.Sprintf("consumer exchange: %s queue: %s routing: %s", s.exchangeName, consumerService.GetQueueName(), strings.Join(consumerService.GetRoutingKeys(), ", ")),
			)),
	}

	for _, routing := range consumerService.GetRoutingKeys() {
		opts = append(opts, rabbitmq.WithConsumerOptionsRoutingKey(routing))
	}

	consumer, err := rabbitmq.NewConsumer(
		conn,
		consumerService.GetQueueName(),
		opts...,
	)
	if err != nil {
		return err
	}

	go func() {
		err = consumer.Run(handler)
		if err != nil {
			slog.Error(fmt.Sprintf("consumer.Run: %v", err))
		}
	}()

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

	s.pubMu.Lock()
	if s.publisher != nil {
		s.publisher.Close()
		s.publisher = nil
	}
	s.pubMu.Unlock()

	s.connMu.Lock()
	defer s.connMu.Unlock()

	if s.connection != nil {
		err := s.connection.Close()
		s.connection = nil
		if err != nil {
			return err
		}
	}

	return nil
}
