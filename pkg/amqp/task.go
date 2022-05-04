package amqp

type Task interface {
	IncrAttemptNumber() int64
}

type BaseTask struct {
	AttemptNumber int64 `json:"attempt_number"`
}

// IncrAttemptNumber инкрементирует количество попыток и возвращает новое значение
func (t *BaseTask) IncrAttemptNumber() int64 {
	t.AttemptNumber++
	return t.AttemptNumber
}
