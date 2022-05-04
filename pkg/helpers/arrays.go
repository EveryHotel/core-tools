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

// StrArrayUniq возвращает только массив строк без дуьлей
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
