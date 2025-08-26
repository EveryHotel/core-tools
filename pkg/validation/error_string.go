package validation

import (
	"strconv"
	"strings"
)

// ParseErrorString парсит строчеку ошибки валидатора и возвращает результат в виде мапа
func ParseErrorString(errMessage string) interface{} {
	idx := strings.Index(errMessage, ":")
	if idx == -1 {
		return errMessage
	}

	res := map[string]interface{}{}
	for idx != -1 {
		var endIdx int
		subField := strings.TrimLeft(errMessage[:idx], " ")

		if string(errMessage[idx+2]) == "(" {

			// Когда несколько пар скобочек находятся на одном уровне, то lastIndex возвращает не парную закрывающую скобочку к текущей, а последнюю
			// По-этому ищем правильную пару для текущей скобочки
			position := idx + 2
			bracketCnt := 0
			for _, c := range errMessage[idx+3:] {
				position += 1
				if string(c) == "(" {
					bracketCnt += 1
				}
				if string(c) == ")" {
					if bracketCnt > 0 {
						bracketCnt -= 1
					} else {
						break
					}
				}
			}
			endIdx = position - 1

			res[subField] = ParseErrorString(errMessage[idx+3 : endIdx])

			if strings.Index(errMessage, ";") != -1 {
				endIdx += 3
			} else {
				endIdx = len(errMessage) // чтобы не было ошибки если это конец сообщения
			}
		} else {
			endIdx = strings.Index(errMessage, ";")
			if endIdx != -1 {
				res[subField] = errMessage[idx+2 : endIdx]
				endIdx += 2
			} else {
				res[subField] = errMessage[idx+2:]
				endIdx = len(errMessage)
			}
		}

		errMessage = errMessage[endIdx:]
		idx = strings.Index(errMessage, ":")
	}

	// Проверяем, является ли текущее поле массивом, для того чтобы вернуть массив в ответе
	// и точно показать в каком элементе оказалась ошибка, пропущенные элементы, где все хорошо
	// помечаем как nil
	isArray := true
	arrKeys := map[int]interface{}{}
	for key, val := range res {
		i, err := strconv.Atoi(key)
		if err != nil {
			isArray = false
			break
		}

		arrKeys[i] = val
	}

	if isArray {
		var arrayRes []interface{}
		for key, val := range arrKeys {
			fillCount := key - len(arrayRes)
			for i := 0; i < fillCount; i++ {
				arrayRes = append(arrayRes, nil)
			}

			arrayRes = append(arrayRes, val)
		}

		return arrayRes
	}

	return res
}
