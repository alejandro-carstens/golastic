package golastic

import (
	"encoding/json"

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

// IsNumeric checks if a value is numeric
func IsNumeric(s interface{}) bool {
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

// IsString checks if a value is a string
func IsString(s interface{}) bool {
	switch s.(type) {
	case string:
		return true
	}

	return false
}

// IsDate checks if a value is a date
func IsDate(s interface{}) bool {
	if IsString(s) {
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
