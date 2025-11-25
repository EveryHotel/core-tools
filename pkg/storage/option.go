package storage

type getOptions struct {
	rangeFrom *int64
	rangeTo   *int64
}

type GetOption func(*getOptions)

func applyOptions(opts []GetOption) *getOptions {
	o := &getOptions{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func WithRangeFrom(from int64) GetOption {
	return func(o *getOptions) {
		o.rangeFrom = &from
	}
}

func WithRangeTo(to int64) GetOption {
	return func(o *getOptions) {
		o.rangeTo = &to
	}
}
