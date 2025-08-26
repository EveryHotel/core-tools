package collection

type Collection[T any, I int64 | string] map[I]T
type keyGetter[T any, I int64 | string] func(T) I

func Collect[T any, I int64 | string](list []T, getter keyGetter[T, I]) Collection[T, I] {
	res := make(Collection[T, I], len(list))
	for _, item := range list {
		res[getter(item)] = item
	}

	return res
}

// Keys возвращает список ключей
func (l Collection[T, I]) Keys() []I {
	var keys []I
	for key, _ := range l {
		keys = append(keys, key)
	}

	return keys
}

// Values возвращает список значений
func (l Collection[T, I]) Values() []T {
	var values []T
	for _, val := range l {
		values = append(values, val)
	}

	return values
}
