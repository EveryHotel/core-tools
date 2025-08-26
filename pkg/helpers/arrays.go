package helpers

// StrArrContains проверяет содержит ли массив строк искомый элемент
func StrArrContains(data []string, needle string) bool {
	for _, item := range data {
		if needle == item {
			return true
		}
	}
	return false
}

// StrArrayUniq возвращает только массив строк без дублей
func StrArrayUniq(slice []string) []string {
	allKeys := make(map[string]struct{})
	var list []string
	for _, item := range slice {
		if _, ok := allKeys[item]; !ok {
			allKeys[item] = struct{}{}
			list = append(list, item)
		}
	}
	return list
}

// ArrIntContains проверяет содержит ли массив целых искомый элемент
func ArrIntContains(data []int64, needle int64) bool {
	for _, item := range data {
		if needle == item {
			return true
		}
	}
	return false
}

// ArrContains проверяет, содержит ли массив нужный элемент
func ArrContains[T int64 | string](data []T, needle T) bool {
	for _, item := range data {
		if needle == item {
			return true
		}
	}
	return false
}

// ArrayDiff возвращает слайс из элементов, которые не содержатся во втором слайсе
func ArrayDiff[T int64 | string](src, dest []T) []T {
	var res []T
	for _, val := range src {
		if ArrContains[T](dest, val) {
			continue
		}

		res = append(res, val)
	}

	return res
}
