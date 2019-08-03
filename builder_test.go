package golastic

import (
	"strconv"
	"testing"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	examples := []interface{}{}

	for i := 1; i < 4; i++ {
		example := new(Example)

		example.Id = strconv.Itoa(i)
		example.Description = "Description " + string(i)
		example.SubjectId = i

		examples = append(examples, example)
	}

	res, err := connection.Builder("example").Insert(examples...)

	if err != nil {
		t.Error("Expected no error on insert ", err)
	}

	assertWriteResponse(t, "create", "created", 201, 1, res)

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got ", err)
	}
}

func TestUpdate(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	example := new(Example)
	example.Id = strconv.Itoa(1)
	example.Description = "Description 1"
	example.SubjectId = 1

	if _, err = connection.Builder("example").Insert(example); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	example.SubjectId = 2
	example.Description = "Description 2"

	res, err := connection.Builder("example").Update(example)

	if err != nil {
		t.Error("Expected no error on update:", err)
	}

	assertWriteResponse(t, "update", "updated", 200, 2, res)

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestDelete(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	example := new(Example)
	example.Id = strconv.Itoa(1)
	example.Description = "Description 1"
	example.SubjectId = 1

	if _, err = connection.Builder("example").Insert(example); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	res, err := connection.Builder("example").Delete(example.Id)

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	assertWriteResponse(t, "delete", "deleted", 200, 2, res)

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestFind(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error got:", err)
	}

	example := new(Example)
	example.Id = strconv.Itoa(20)
	example.SubjectId = 1
	example.Description = "Description 1"

	if _, err = connection.Builder("example").Insert(example); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	var response Example

	if err = connection.Builder("example").Find(example.Id, &response); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	assert.Equal(t, example.Id, response.Id)
	assert.Equal(t, example.SubjectId, response.SubjectId)
	assert.Equal(t, example.Description, response.Description)

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestUpdateByQuery(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	example := new(Example)

	example.Id = strconv.Itoa(20)
	example.Description = "Description 1"
	example.SubjectId = 1

	builder := connection.Builder("example")

	if _, err = builder.Insert(example); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	time.Sleep(1 * time.Second)

	builder.Where("Id", "<>", 2)

	response, err := builder.Execute(map[string]interface{}{"SubjectId": 20})

	if err != nil {
		t.Error("Expected no error got:", err)
	}

	assert.Equal(t, false, response.TimedOut)
	assert.Equal(t, 1, response.Total)
	assert.Equal(t, 1, response.Updated)
	assert.Equal(t, 0, response.Deleted)
	assert.Equal(t, 1, response.Batches)
	assert.Equal(t, 0, response.VersionConflicts)
	assert.Equal(t, 0, response.Noops)
	assert.Equal(t, 0, response.Retries.Bulk)
	assert.Equal(t, 0, response.Retries.Search)
	assert.Equal(t, "", response.Throttled)
	assert.Equal(t, 0, response.ThrottledMillis)
	assert.Equal(t, -1, response.RequestsPerSecond)
	assert.Equal(t, "", response.ThrottledUntil)
	assert.Equal(t, 0, response.ThrottledUntilMillis)
	assert.Equal(t, 0, len(response.Failures))

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestDeleteByQuery(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	example := new(Example)

	example.Id = strconv.Itoa(20)
	example.Description = "Description 1"
	example.SubjectId = 1

	builder := connection.Builder("example")

	if _, err = builder.Insert(example); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	time.Sleep(1 * time.Second)

	builder.Where("Id", "<>", 2)
	response, err := builder.Destroy()

	if err != nil {
		t.Error("Expected no error got:", err)
	}

	assert.Equal(t, false, response.TimedOut)
	assert.Equal(t, 1, response.Total)
	assert.Equal(t, 0, response.Updated)
	assert.Equal(t, 1, response.Deleted)
	assert.Equal(t, 1, response.Batches)
	assert.Equal(t, 0, response.VersionConflicts)
	assert.Equal(t, 0, response.Noops)
	assert.Equal(t, 0, response.Retries.Bulk)
	assert.Equal(t, 0, response.Retries.Search)
	assert.Equal(t, "", response.Throttled)
	assert.Equal(t, 0, response.ThrottledMillis)
	assert.Equal(t, -1, response.RequestsPerSecond)
	assert.Equal(t, "", response.ThrottledUntil)
	assert.Equal(t, 0, response.ThrottledUntilMillis)
	assert.Equal(t, 0, len(response.Failures))

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestGetWheres(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	builder := connection.Builder("example")

	if _, err = builder.Insert(getModelsToSeedAsInterface(10)...); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	notInDescriptions := []interface{}{
		"Description 2",
		"Description 4",
	}

	time.Sleep(1 * time.Second)

	builder.Where("id", "<>", 3).
		Where("subject_id", "<", 2).
		WhereNotIn("description", notInDescriptions).
		Where("description", "<>", "Description 5")

	var response []Example

	if err := builder.Get(&response); err != nil {
		t.Error("Expected no error got:", err)
	}

	assert.Equal(t, 1, len(response))
	assert.Equal(t, "1", response[0].Id)
	assert.Equal(t, 1, response[0].SubjectId)
	assert.Equal(t, "Description 1", response[0].Description)

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestGetFilters(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	builder := connection.Builder("example")

	if _, err = builder.Insert(getModelsToSeedAsInterface(10)...); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	time.Sleep(1 * time.Second)

	builder.Where("id", "<>", 3).FilterIn("subject_id", []interface{}{1}).Filter("id", "<", 2)

	var response []Example

	if err := builder.Get(&response); err != nil {
		t.Error("Expected no error got:", err)
	}

	assert.Equal(t, 1, len(response))
	assert.Equal(t, "1", response[0].Id)
	assert.Equal(t, 1, response[0].SubjectId)
	assert.Equal(t, "Description 1", response[0].Description)

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestGetMatches(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	builder := connection.Builder("example")

	if _, err = builder.Insert(getModelsToSeedAsInterface(10)...); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	time.Sleep(1 * time.Second)

	notInIds := []interface{}{2}

	builder.MatchNotIn("id", notInIds).
		MatchIn("subject_id", notInIds).
		Match("description", "<>", "Description 9").
		Match("description", "=", "Description 10")

	var response []Example

	if err := builder.Get(&response); err != nil {
		t.Error("Expected no error got:", err)
	}

	assert.Equal(t, 1, len(response))
	assert.Equal(t, "10", response[0].Id)
	assert.Equal(t, 2, response[0].SubjectId)
	assert.Equal(t, "Description 10", response[0].Description)

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestGetLimitAndSort(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error on insert:", err)
	}

	builder := connection.Builder("example")

	if _, err = builder.Insert(getModelsToSeedAsInterface(10)...); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	inDescriptions := []interface{}{
		"Description 2",
		"Description 4",
		"Description 6",
		"Description 8",
	}

	time.Sleep(1 * time.Second)

	builder.WhereIn("description", inDescriptions).OrderBy("id", false).Limit(2)

	var response []Example

	if err := builder.Get(&response); err != nil {
		t.Error("Expected no error got:", err)
	}

	assert.Equal(t, 2, len(response))
	assert.Equal(t, "8", response[0].Id)
	assert.Equal(t, 2, response[0].SubjectId)
	assert.Equal(t, "Description 8", response[0].Description)
	assert.Equal(t, "6", response[1].Id)
	assert.Equal(t, 2, response[1].SubjectId)
	assert.Equal(t, "Description 6", response[1].Description)

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestAggregationGroupBy(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error on insert:", err)
	}

	builder := connection.Builder("example")

	if _, err = builder.Insert(getModelsToSeedAsInterface(11)...); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	inDescriptions := []interface{}{
		"Description 2",
		"Description 4",
		"Description 6",
		"Description 8",
		"Description 11",
	}

	time.Sleep(1 * time.Second)

	builder.WhereIn("description", inDescriptions).GroupBy("subject_id")

	response, err := builder.Aggregate()

	if err != nil {
		t.Error("Expected no error on insert:", err)
	}

	aggrecation := response["subject_id"]

	assert.Equal(t, 1, int(aggrecation.Buckets[0].Key.(float64)))
	assert.Equal(t, 3, aggrecation.Buckets[0].DocCount)
	assert.Equal(t, 2, int(aggrecation.Buckets[1].Key.(float64)))
	assert.Equal(t, 2, aggrecation.Buckets[1].DocCount)

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestAggregationGroupByMany(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error on insert:", err)
	}

	builder := connection.Builder("example")

	if _, err = builder.Insert(getModelsToSeedAsInterface(11)...); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	inDescriptions := []interface{}{"Description 4", "Description 8", "Description 11"}

	time.Sleep(1 * time.Second)

	fields := []string{"id", "subject_id", "description"}

	builder.FilterIn("description", inDescriptions).GroupBy(fields...)

	response, err := builder.Aggregate()

	if err != nil {
		t.Error("Expected no error on insert:", err)
	}

	aggregation := response["id"]

	assert.Equal(t, 1, aggregation.Buckets[0].DocCount)
	assert.Equal(t, "11", aggregation.Buckets[0].Key.(string))
	assert.Equal(t, "Description 11", aggregation.Buckets[0].Items["description"].Buckets[0].Key.(string))
	assert.Equal(t, 1, int(aggregation.Buckets[0].Items["subject_id"].Buckets[0].Key.(float64)))
	assert.Equal(t, 1, aggregation.Buckets[1].DocCount)
	assert.Equal(t, "4", aggregation.Buckets[1].Key.(string))
	assert.Equal(t, "Description 4", aggregation.Buckets[1].Items["description"].Buckets[0].Key.(string))
	assert.Equal(t, 1, int(aggregation.Buckets[1].Items["subject_id"].Buckets[0].Key.(float64)))
	assert.Equal(t, 1, aggregation.Buckets[2].DocCount)
	assert.Equal(t, "8", aggregation.Buckets[2].Key.(string))
	assert.Equal(t, "Description 8", aggregation.Buckets[2].Items["description"].Buckets[0].Key.(string))
	assert.Equal(t, 2, int(aggregation.Buckets[2].Items["subject_id"].Buckets[0].Key.(float64)))

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestCount(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error on insert:", err)
	}

	builder := connection.Builder("example")

	if _, err = builder.Insert(getModelsToSeedAsInterface(11)...); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	inDescriptions := []interface{}{"Description 4", "Description 8", "Description 11"}

	time.Sleep(1 * time.Second)

	builder.FilterIn("description", inDescriptions)

	response, err := builder.Count()

	if err != nil {
		t.Error("Expected no error got:", err)
	}

	assert.Equal(t, int64(3), response)

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestCursor(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error on insert:", err)
	}

	builder := connection.Builder("example")

	if _, err = builder.Insert(getModelsToSeedAsInterface(15)...); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	time.Sleep(1 * time.Second)

	builder.Filter("subject_id", "=", 1).OrderBy("id", true).OrderBy("description", true)

	var response []Example

	sortValues, err := builder.Cursor(2, nil, &response)

	if err != nil {
		t.Error("Expected no error got:", err)
	}

	assert.True(t, len(response) == 2)
	assert.Equal(t, "1", response[0].Id)
	assert.Equal(t, "11", response[1].Id)

	response = []Example{}

	if _, err = builder.Cursor(1, sortValues, &response); err != nil {
		t.Error("Expected no error got:", err)
	}

	assert.True(t, len(response) == 1)
	assert.Equal(t, "12", response[0].Id)

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestFromGet(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error on insert:", err)
	}

	builder := connection.Builder("example")

	if _, err = builder.Insert(getModelsToSeedAsInterface(15)...); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	time.Sleep(1 * time.Second)

	builder.Filter("subject_id", "=", 1).OrderBy("id", true).Limit(5).From(5)

	response := []Example{}

	if err = builder.Get(&response); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	assert.True(t, len(response) == 5)
	assert.Equal(t, response[0].Id, "15")
	assert.Equal(t, response[1].Id, "2")
	assert.Equal(t, response[2].Id, "3")
	assert.Equal(t, response[3].Id, "4")
	assert.Equal(t, response[4].Id, "5")

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestMinMax(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error on insert:", err)
	}

	builder := connection.Builder("example")

	if _, err = builder.Insert(getModelsToSeedAsInterface(15)...); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	time.Sleep(1 * time.Second)

	result, err := builder.MinMax("subject_id", false)

	if err != nil {
		t.Error("Expected no error on aggs query:", err)
	}

	assert.Equal(t, 1, int(result.Min.(float64)))
	assert.Equal(t, 2, int(result.Max.(float64)))

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func initConnection() (*connection, error) {
	connection, err := bootConnection()

	if err != nil {
		return nil, err
	}

	if err := connection.Indexer(nil).CreateIndex("example", indexConfig()); err != nil {
		return nil, err
	}

	return connection, nil
}

func tearDownBuilder(connection *connection) error {
	return connection.Indexer(nil).DeleteIndex("example")
}

func assertWriteResponse(t *testing.T, action string, result string, status int, version int, response *gabs.Container) {
	items, err := response.S("items").Children()

	if err != nil {
		t.Error(err)
	}

	assert.True(t, len(items) > 0)

	for i, item := range items {
		i++

		assert.Equal(t, strconv.Itoa(i), item.S(action, "_id").Data().(string))
		assert.Equal(t, "example", item.S(action, "_index").Data().(string))
		assert.Equal(t, "_doc", item.S(action, "_type").Data().(string))
		assert.Equal(t, result, item.S(action, "result").Data().(string))
		assert.Equal(t, float64(version), item.S(action, "_version").Data().(float64))
		assert.Equal(t, float64(1), item.S(action, "_shards", "successful").Data().(float64))
		assert.Equal(t, float64(0), item.S(action, "_shards", "failed").Data().(float64))
		assert.Equal(t, float64(1), item.S(action, "_primary_term").Data().(float64))
		assert.Equal(t, float64(status), item.S(action, "status").Data().(float64))
	}
}

func getModelsToSeedAsInterface(num int) []interface{} {
	models := []interface{}{}

	for i := 0; i < num; i++ {
		model := new(Example)

		id := strconv.Itoa(i + 1)

		model.Id = id
		model.Description = "Description " + id
		model.SubjectId = 2

		if i < 5 || i > 9 {
			model.SubjectId = 1
		}

		models = append(models, model)
	}

	return models
}
