package golastic

import (
	"strconv"
	"testing"
)

func TestInsertGolasticModel(t *testing.T) {
	elasticsearchBuilder, err := InitBuilder()

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	examples := []ElasticModelable{}
	data := map[string]interface{}{}

	for i := 1; i < 4; i++ {
		example := new(GolasticModel)
		example.SetIndex("example")
		data["id"] = strconv.Itoa(i)
		data["description"] = "Description " + string(i)
		data["subjectId"] = i

		example.SetData(data)
		example.SetIsGolasticModel(true)

		examples = append(examples, example)
	}

	res, err := elasticsearchBuilder.Insert(examples...)

	if err != nil {
		t.Error("Expected no error on insert ", err)
	}

	assertInsertResponse(t, res)

	if err := TearDownBuilder(elasticsearchBuilder); err != nil {
		t.Error("Expected no error got ", err)
	}
}
