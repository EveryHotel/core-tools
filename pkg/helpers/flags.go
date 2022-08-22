package helpers

type Bits interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64
}

// HasFlags проверяет присутствует ли в битовой маске биты из flag
func HasFlags[T Bits](value, flags T) bool {
	return (value & flags) != 0
}

// AddFlags возвращает новую битовую маску с установлинными битами из flags
func AddFlags[T Bits](value, flags T) T {
	return value | flags
}

// RemoveFlags возвращает новую битовую маску без битов из flags
func RemoveFlags[T Bits](value, flags T) T {
	return value &^ flags
}
