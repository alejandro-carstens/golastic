package golastic

import (
	"encoding/json"
	"math"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/araddon/dateparse"
)

// NewConnection creates an elasticsearch connection
func NewConnection(context *ConnectionContext) *Connection {
	return &Connection{
		context: context,
	}
}

func toJson(item interface{}) (string, error) {
	value, err := json.Marshal(item)

	return string(value), err
}

func fromJson(value string, entity interface{}) (interface{}, error) {
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

func inSlice(needle string, haystack ...string) bool {
	for _, value := range haystack {
		if needle == value {
			return true
		}
	}

	return false
}

func isDate(s interface{}) bool {
	if isString(s) {
		if _, err := dateparse.ParseAny(s.(string)); err != nil {
			return false
		}

		return true
	} else if _, valid := s.(time.Time); valid {
		return true
	}

	return false
}

func sliceRemove(n int, slice []string) []string {
	arr := []string{}

	for i, val := range slice {
		if i == n {
			continue
		}

		arr = append(arr, val)
	}

	return arr
}

func parse(response interface{}, err error) (*gabs.Container, error) {
	if err != nil {
		return nil, err
	}

	return toGabsContainer(response)
}

func toGabsContainer(value interface{}) (*gabs.Container, error) {
	b, err := json.Marshal(value)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func calculateChunkSize(length int) int {
	return (length + CONCURRENT_BATCH - 1) / CONCURRENT_BATCH
}

func calculateChunkCount(length int, chunkSize int) int {
	return int(math.Ceil(float64(length) / float64(chunkSize)))
}
