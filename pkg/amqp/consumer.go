package amqp

import "github.com/wagslane/go-rabbitmq"

type ConsumerHandler interface {
	Handle(rabbitmq.Delivery) (Task, rabbitmq.Action)
}

type ConsumerService interface {
	GetHandler() ConsumerHandler
	GetQueueName() string
	GetRoutingKeys() []string
	GetMaxAttempts() int64
	GetConcurrency() int

	SetMaxAttempts(int64)
	SetConcurrency(int)
}

type ConsumerOption func(service ConsumerService)

// WithMaxAttempts задает максимальное число попыток для выполнения задач
func WithMaxAttempts(num int64) ConsumerOption {
	return func(consumer ConsumerService) {
		consumer.SetMaxAttempts(num)
	}
}

// WithConcurrency количество запускаемых воркеров для обслуживания задач
func WithConcurrency(num int) ConsumerOption {
	return func(consumer ConsumerService) {
		consumer.SetConcurrency(num)
	}
}

type consumerService struct {
	handler     ConsumerHandler
	queueName   string
	routingKeys []string
	maxAttempts int64
	concurrency int
}

func (s *consumerService) SetMaxAttempts(num int64) {
	s.maxAttempts = num
}

func (s *consumerService) SetConcurrency(num int) {
	s.concurrency = num
}

func (s consumerService) GetHandler() ConsumerHandler {
	return s.handler
}

func (s consumerService) GetRoutingKeys() []string {
	return s.routingKeys
}

func (s consumerService) GetMaxAttempts() int64 {
	return s.maxAttempts
}

func (s consumerService) GetConcurrency() int {
	return s.concurrency
}

func (s consumerService) GetQueueName() string {
	return s.queueName
}

func NewConsumerService(handler ConsumerHandler, queueName string, routingKeys []string, options ...ConsumerOption) ConsumerService {
	consumer := &consumerService{
		handler:     handler,
		routingKeys: routingKeys,
		queueName:   queueName,
		concurrency: 1,
	}

	for _, opt := range options {
		opt(consumer)
	}

	return consumer
}
