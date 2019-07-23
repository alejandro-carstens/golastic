package golastic

import (
	"testing"
)

type Example struct {
	ElasticModel
	Description string `json:"Description,omitempty"`
	SubjectId   int    `json:"SubjectId,omitempty"`
}

func (ex *Example) New() *Example {
	ex.Properties()
	ex.Index()
	ex.PropertiesMap()

	return ex
}

func (ex *Example) Properties() []string {
	ex.properties = []string{
		"Id",
		"Description",
		"SubjectId",
	}

	return ex.properties
}

func (ex *Example) Index() string {
	ex.index = "example"

	return ex.index
}

func (ex *Example) PropertiesMap() map[string]interface{} {
	ex.propertiesMap = map[string]interface{}{
		"Id": map[string]interface{}{
			"type":  "keyword",
			"index": true,
		},
		"Description": map[string]interface{}{
			"type":  "keyword",
			"index": true,
		},
		"SubjectId": map[string]interface{}{
			"type":  "integer",
			"index": true,
		},
	}

	return ex.propertiesMap
}

func (ex *Example) SetId() *Example {
	ex.Id = ex.GenerateId()

	return ex
}

func TestCreateExistsDestroyIndex(t *testing.T) {
	example := new(Example).New()

	indexer := new(indexer)

	if err := indexer.Init(); err != nil {
		t.Error("Expected to client, got ", err)
	}

	exists, err := indexer.Exists(example.Index())

	if exists == true {
		t.Error("Expected index not found got ", exists)
	} else if err != nil {
		t.Error("Expected index not found got ", err)
	}

	mappings, err := ElasticSearchIndexConfig(1, 0, example.PropertiesMap())

	if err != nil {
		t.Error("Expected no errors on map creation got ", err)
	}

	if err := indexer.CreateIndex(example.Index(), mappings); err != nil {
		t.Error("Expected index got created got", err)
	}

	exists, err = indexer.Exists(example.Index())

	if exists == false {
		t.Error("Expected index found got ", exists)
	} else if err != nil {
		t.Error("Expected index found got ", err)
	}

	if err := indexer.DeleteIndex(example.Index()); err != nil {
		t.Error("Expected index got deleted got", err)
	}
}
