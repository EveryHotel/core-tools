package helpers

import (
	"context"
)

type Pipeline[T any] func(ctx context.Context, item T) (T, bool, error)

type PipelineService[T any] interface {
	Handle(ctx context.Context, item T) (T, bool, error)
}

type pipelineService[T any] struct {
	pipelines []Pipeline[T]
}

func NewPipelineService[T any](pipelines ...Pipeline[T]) PipelineService[T] {
	return &pipelineService[T]{
		pipelines: pipelines,
	}
}

func (p *pipelineService[T]) Handle(ctx context.Context, item T) (T, bool, error) {
	var ok bool
	var err error
	for _, pipe := range p.pipelines {
		item, ok, err = pipe(ctx, item)
		if !ok || err != nil {
			return item, ok, err
		}
	}
	return item, ok, err
}
