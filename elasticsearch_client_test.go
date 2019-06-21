package golastic

import "testing"

func TestSetClient(t *testing.T) {
	elsaticsearchClient := new(ElasticsearchClient)

	if err := elsaticsearchClient.InitClient(); err != nil {
		t.Error("Expected index not found got ", err)
	}
}
