package golastic

import (
	"encoding/json"
	"errors"

	"github.com/araddon/dateparse"
)

func Connection(context *ConnectionContext) *connection {
	return &connection{
		context: context,
	}
}

// ToJson encodes json.Marshal to a string
func ToJson(item interface{}) (string, error) {
	value, err := json.Marshal(item)

	return string(value), err
}

// FromJson performs a json.Unmarshal
func FromJson(value string, entity interface{}) (interface{}, error) {
	err := json.Unmarshal([]byte(value), &entity)

	return entity, err
}

func isNumeric(s interface{}) bool {
	switch s.(type) {
	case int:
		return true
	case int32:
		return true
	case float32:
		return true
	case float64:
		return true
	}

	return false
}

func isString(s interface{}) bool {
	switch s.(type) {
	case string:
		return true
	}

	return false
}

func isValidOperand(operand string) error {
	if operand == "<>" || operand == "=" || operand == ">" || operand == "<" || operand == "<=" || operand == ">=" {
		return nil
	}

	return errors.New("The operand is invalid.")
}

func inSlice(needle string, haystack ...string) bool {
	for _, value := range haystack {
		if needle == value {
			return true
		}
	}

	return false
}

// IsDate checks if a value is a date
func IsDate(s interface{}) bool {
	if isString(s) {
		_, err := dateparse.ParseAny(s.(string))

		if err != nil {
			return false
		}

		return true
	}

	return false
}

// SliceRemove unsets string at index s from a string slice
func SliceRemove(n int, slice []string) []string {
	arr := []string{}

	for i, val := range slice {
		if i == n {
			continue
		}

		arr = append(arr, val)
	}

	return arr
}
