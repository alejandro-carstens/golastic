package golastic

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Example struct {
	Id          string `json:"id"`
	Description string `json:"description,omitempty"`
	SubjectId   int    `json:"subject_id,omitempty"`
}

func (e *Example) ID() string {
	return e.Id
}

func indexConfig() string {
	schema, _ := toJson(map[string]interface{}{
		"settings": map[string]int{
			"number_of_shards":   1,
			"number_of_replicas": 1,
		},
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"id": map[string]interface{}{
					"type":  "keyword",
					"index": true,
				},
				"description": map[string]interface{}{
					"type":  "keyword",
					"index": true,
				},
				"subject_id": map[string]interface{}{
					"type":  "integer",
					"index": true,
				},
			},
		},
	})

	return schema
}

func bootConnection() (*Connection, error) {
	connection := NewConnection(
		&ConnectionContext{
			Urls:                []string{os.Getenv("ELASTICSEARCH_URI")},
			Password:            os.Getenv("ELASTICSEARCH_PASSWORD"),
			Username:            os.Getenv("ELASTICSEARCH_USERNAME"),
			HealthCheckInterval: 30,
		},
	)

	err := connection.Connect()

	return connection, err
}

func TestCreateExistsDestroyIndex(t *testing.T) {
	connection, err := bootConnection()

	if err != nil {
		t.Error(err)
	}

	indexer := connection.Indexer(nil)

	exists, err := indexer.Exists("example")

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	assert.False(t, exists)

	if err := indexer.CreateIndex("example", indexConfig()); err != nil {
		t.Error("Expected index got created got", err)
	}

	exists, err = indexer.Exists("example")

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	assert.True(t, exists)

	if err := indexer.DeleteIndex("example"); err != nil {
		t.Error("Expected index got deleted got", err)
	}
}
