package golastic

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	examples := []*Example{}

	for i := 1; i < 4; i++ {
		example := new(Example)

		example.Id = strconv.Itoa(i)
		example.Description = "Description " + string(i)
		example.SubjectId = i

		examples = append(examples, example)
	}

	res, err := connection.Builder("example").Insert(examples)

	if err != nil {
		t.Error("Expected no error on insert ", err)
	}

	assertInsertResponse(t, res)

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
	example.Id = strconv.Itoa(20)
	example.Description = "Description 1"
	example.SubjectId = 1

	if _, err = connection.Builder("example").Insert(example); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	example.SubjectId = 2
	example.Description = "Description 2"

	response, err := connection.Builder("example").Update(example)

	if err != nil {
		t.Error("Expected no error on update:", err)
	}

	assert.True(t, len(response.GetItems()) == 1)
	assert.Equal(t, strconv.Itoa(20), response.First().Id)
	assert.Equal(t, "example", response.First().Index)
	assert.Equal(t, "_doc", response.First().Type)
	assert.Equal(t, "updated", response.First().Result)
	assert.Equal(t, 2, response.First().Version)
	assert.Equal(t, 1, response.First().Shards.Total)
	assert.Equal(t, 1, response.First().Shards.Successful)
	assert.Equal(t, 0, response.First().Shards.Failed)
	assert.Equal(t, 1, response.First().PrimaryTerm)
	assert.Equal(t, 200, response.First().Status)

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

func TestDelete(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	example := new(Example)
	example.Id = strconv.Itoa(20)
	example.Description = "Description 1"
	example.SubjectId = 1

	if _, err = connection.Builder("example").Insert(example); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	response, err := connection.Builder("example").Delete(example.Id)

	if err != nil {
		t.Error("Expected no error got ", err)
	}

	assert.True(t, len(response.GetItems()) == 1)
	assert.Equal(t, strconv.Itoa(20), response.First().Id)
	assert.Equal(t, "example", response.First().Index)
	assert.Equal(t, "_doc", response.First().Type)
	assert.Equal(t, "deleted", response.First().Result)
	assert.Equal(t, 2, response.First().Version)
	assert.Equal(t, 1, response.First().Shards.Total)
	assert.Equal(t, 1, response.First().Shards.Successful)
	assert.Equal(t, 0, response.First().Shards.Failed)
	assert.Equal(t, 1, response.First().PrimaryTerm)
	assert.Equal(t, 200, response.First().Status)

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

	models := getModelsToSeed(10)

	builder := connection.Builder("example")

	if _, err = builder.Insert(models); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	notInDescriptions := []interface{}{
		"Description 2",
		"Description 4",
	}

	time.Sleep(1 * time.Second)

	builder.Where("Id", "<>", 3).
		Where("SubjectId", "<", 2).
		WhereNotIn("Description", notInDescriptions).
		Where("Description", "<>", "Description 5")

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

	models := getModelsToSeed(10)

	builder := connection.Builder("example")

	if _, err = builder.Insert(models); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	time.Sleep(1 * time.Second)

	inSubjectIds := []interface{}{
		1,
	}

	builder.Where("Id", "<>", 3).FilterIn("SubjectId", inSubjectIds).Filter("Id", "<", 2)

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

	models := getModelsToSeed(10)

	builder := connection.Builder("example")

	if _, err = builder.Insert(models); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	time.Sleep(1 * time.Second)

	notInIds := []interface{}{
		2,
	}

	builder.MatchNotIn("Id", notInIds).
		MatchIn("SubjectId", notInIds).
		Match("Description", "<>", "Description 9").
		Match("Description", "=", "Description 10")

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

	models := getModelsToSeed(10)

	builder := connection.Builder("example")

	if _, err = builder.Insert(models); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	inDescriptions := []interface{}{
		"Description 2",
		"Description 4",
		"Description 6",
		"Description 8",
	}

	time.Sleep(1 * time.Second)

	builder.WhereIn("Description", inDescriptions).OrderBy("Id", false).Limit(2)

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

	models := getModelsToSeed(11)

	builder := connection.Builder("example")

	if _, err = builder.Insert(models); err != nil {
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

	builder.WhereIn("Description", inDescriptions).GroupBy("SubjectId")

	response, err := builder.Aggregate()

	if err != nil {
		t.Error("Expected no error on insert:", err)
	}

	aggrecation := response["SubjectId"]

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

	models := getModelsToSeed(11)

	builder := connection.Builder("example")

	if _, err = builder.Insert(models); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	inDescriptions := []interface{}{
		"Description 4",
		"Description 8",
		"Description 11",
	}

	time.Sleep(1 * time.Second)

	fields := []string{"Id", "SubjectId", "Description"}

	builder.FilterIn("Description", inDescriptions).GroupBy(fields...)

	response, err := builder.Aggregate()

	if err != nil {
		t.Error("Expected no error on insert:", err)
	}

	aggregation := response["Id"]

	assert.Equal(t, 1, aggregation.Buckets[0].DocCount)
	assert.Equal(t, "11", aggregation.Buckets[0].Key.(string))
	assert.Equal(t, "Description 11", aggregation.Buckets[0].Items["Description"].Buckets[0].Key.(string))
	assert.Equal(t, 1, int(aggregation.Buckets[0].Items["SubjectId"].Buckets[0].Key.(float64)))
	assert.Equal(t, 1, aggregation.Buckets[1].DocCount)
	assert.Equal(t, "4", aggregation.Buckets[1].Key.(string))
	assert.Equal(t, "Description 4", aggregation.Buckets[1].Items["Description"].Buckets[0].Key.(string))
	assert.Equal(t, 1, int(aggregation.Buckets[1].Items["SubjectId"].Buckets[0].Key.(float64)))
	assert.Equal(t, 1, aggregation.Buckets[2].DocCount)
	assert.Equal(t, "8", aggregation.Buckets[2].Key.(string))
	assert.Equal(t, "Description 8", aggregation.Buckets[2].Items["Description"].Buckets[0].Key.(string))
	assert.Equal(t, 2, int(aggregation.Buckets[2].Items["SubjectId"].Buckets[0].Key.(float64)))

	if err := tearDownBuilder(connection); err != nil {
		t.Error("Expected no error got:", err)
	}
}

func TestCount(t *testing.T) {
	connection, err := initConnection()

	if err != nil {
		t.Error("Expected no error on insert:", err)
	}

	models := getModelsToSeed(11)

	builder := connection.Builder("example")

	if _, err = builder.Insert(models); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	inDescriptions := []interface{}{
		"Description 4",
		"Description 8",
		"Description 11",
	}

	time.Sleep(1 * time.Second)

	builder.FilterIn("Description", inDescriptions)

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

	models := getModelsToSeed(15)

	builder := connection.Builder("example")

	if _, err = builder.Insert(models); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	time.Sleep(1 * time.Second)

	builder.Filter("SubjectId", "=", 1).OrderBy("Id", true).OrderBy("Description", true)

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

	models := getModelsToSeed(15)

	builder := connection.Builder("example")

	if _, err = builder.Insert(models); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	time.Sleep(1 * time.Second)

	builder.Filter("SubjectId", "=", 1).OrderBy("Id", true).Limit(5).From(5)

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

	models := getModelsToSeed(15)

	builder := connection.Builder("example")

	if _, err = builder.Insert(models); err != nil {
		t.Error("Expected no error on insert:", err)
	}

	time.Sleep(1 * time.Second)

	result, err := builder.MinMax("SubjectId", false)

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

func assertInsertResponse(t *testing.T, response *WriteResponse) {
	assert.IsType(t, 1, response.Took)
	assert.True(t, len(response.GetItems()) > 0)

	for i, insert := range response.GetItems() {
		i++

		assert.Equal(t, strconv.Itoa(i), insert.Id)
		assert.Equal(t, "example", insert.Index)
		assert.Equal(t, "_doc", insert.Type)
		assert.Equal(t, "created", insert.Result)
		assert.Equal(t, 1, insert.Version)
		assert.Equal(t, 1, insert.Shards.Successful)
		assert.Equal(t, 0, insert.Shards.Failed)
		assert.Equal(t, 1, insert.PrimaryTerm)
		assert.Equal(t, 201, insert.Status)
	}
}

func getModelsToSeed(num int) []Identifiable {
	var models []Identifiable

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
