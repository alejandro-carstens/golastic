package golastic

import (
	"errors"
	"time"

	"github.com/segmentio/ksuid"
)

type ElasticModel struct {
	Id              string `json:"Id"`
	mappings        map[string]interface{}
	properties      []string
	propertiesMap   map[string]interface{}
	index           string
	data            map[string]interface{}
	isGolasticModel bool
}

func (em *ElasticModel) GetIndexMappings() map[string]interface{} {
	return map[string]interface{}{
		"_doc": map[string]interface{}{
			"properties": em.propertiesMap,
		},
	}
}

func (em *ElasticModel) GenerateId() string {
	return ksuid.New().String()
}

func (em *ElasticModel) Timestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func (em *ElasticModel) GetId() string {
	return em.Id
}

func (em *ElasticModel) Validate() error {
	if !em.IsGolasticModel() && em.index == "" {
		return errors.New("Index cannot be empty")
	}

	return nil
}

func (em *ElasticModel) Data() map[string]interface{} {
	return em.data
}

func (em *ElasticModel) SetIsGolasticModel(val bool) {
	em.isGolasticModel = val
}

func (em *ElasticModel) IsGolasticModel() bool {
	return em.isGolasticModel
}
