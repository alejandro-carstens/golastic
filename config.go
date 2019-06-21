package golastic

func ElasticSearchIndexConfig(numberOfShards int, numberOfReplicas int, mappings map[string]interface{}) (string, error) {
	config := map[string]interface{}{
		"settings": map[string]int{
			"number_of_shards":   numberOfShards,
			"number_of_replicas": numberOfReplicas,
		},
		"mappings": map[string]interface{}{
			"properties": mappings,
		},
	}

	return ToJson(config)
}
