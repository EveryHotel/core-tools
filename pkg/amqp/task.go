package amqp

type Task interface {
	IncrAttemptNumber() int64
	GetFailedCallback() func()
}

type BaseTask struct {
	AttemptNumber  int64  `json:"attempt_number"`
	FailedCallback func() `json:"-"`
}

// IncrAttemptNumber инкрементирует количество попыток и возвращает новое значение
func (t *BaseTask) IncrAttemptNumber() int64 {
	t.AttemptNumber++
	return t.AttemptNumber
}

// GetFailedCallback возвращает функцию, которая срабатывает в случае неудачного выполнения задачи после всех попыток
func (t BaseTask) GetFailedCallback() func() {
	return t.FailedCallback
}
