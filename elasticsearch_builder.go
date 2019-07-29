package golastic

import (
	"context"
	"encoding/json"
	"errors"
	"math"

	"github.com/rs/xid"

	"github.com/Jeffail/gabs"
	elastic "github.com/alejandro-carstens/elasticfork"
)

const CONCURRENT_BATCH int = 10
const LIMIT int = 10000

type ElasticsearchBuilder struct {
	queryBuilder
	client        *elastic.Client
	searchService *elastic.SearchService
	index         string
}

// Find retrieves an instance of a model for the specified Id from the corresponding elasticsearch index
func (esb *ElasticsearchBuilder) Find(id string, model interface{}) error {
	ctx := context.Background()

	response, err := esb.client.Get().Index(esb.index).Id(id).Do(ctx)

	if err != nil {
		return err
	}

	if response.Found == false {
		return errors.New("No document found for model with id:" + id)
	}

	data, err := response.Source.MarshalJSON()

	if err != nil {
		return err
	}

	return json.Unmarshal(data, model)
}

// Insert inserts one or multiple documents into the corresponding elasticsearch index
func (esb *ElasticsearchBuilder) Insert(items ...interface{}) (*WriteResponse, error) {
	batchClient := esb.client.Bulk()

	for _, item := range items {
		req := elastic.NewBulkIndexRequest().Index(esb.index).Id(xid.New().String()).OpType("create")

		batchClient = batchClient.Add(req.Doc(item))
	}

	return esb.processBulkRequest(batchClient, len(items))
}

// Delete deletes one or multiple documents by id from the corresponding elasticsearch index
func (esb *ElasticsearchBuilder) Delete(ids ...string) (*WriteResponse, error) {
	batchClient := esb.client.Bulk()

	for _, id := range ids {
		req := elastic.NewBulkDeleteRequest().Index(esb.index).Id(id)

		batchClient = batchClient.Add(req)
	}

	return esb.processBulkRequest(batchClient, len(ids))
}

// Update updates one or multiple documents from the corresponding elasticsearch index
func (esb *ElasticsearchBuilder) Update(items ...Identifiable) (*WriteResponse, error) {
	batchClient := esb.client.Bulk()

	for _, item := range items {
		req := elastic.NewBulkUpdateRequest().Index(esb.index).Id(item.ID())

		batchClient = batchClient.Add(req.Doc(item))
	}

	return esb.processBulkRequest(batchClient, len(items))
}

// Aggregate retrieves all the queries aggregations
func (esb *ElasticsearchBuilder) Aggregate() (map[string]*AggregationResponse, error) {
	searchService, err := esb.build()

	if err != nil {
		return nil, err
	}

	response, err := searchService.Do(context.Background())

	if err != nil {
		return nil, err
	}

	if response.Aggregations == nil {
		return nil, errors.New("No aggregations returned")
	}

	return esb.processAggregations(response.Aggregations)
}

// Get executes the search query and retrieves the results
func (esb *ElasticsearchBuilder) Get(items interface{}) error {
	searchService, err := esb.build()

	if err != nil {
		return err
	}

	response, err := searchService.Do(context.Background())

	if err != nil {
		return err
	}

	sources := esb.processGetResults(response.Hits.Hits)

	results, err := ToJson(sources)

	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(results), items)
}

// Execute executes an ubdate by query
func (esb *ElasticsearchBuilder) Execute(params map[string]interface{}) (*WriteByQueryResponse, error) {
	query, err := esb.updateByQuery()

	if err != nil {
		return nil, err
	}

	script := esb.buildScript(params)

	updateResponse, err := query.Script(script).Refresh("true").Do(context.Background())

	if err != nil {
		return nil, err
	}

	result, err := ToJson(updateResponse)

	if err != nil {
		return nil, err
	}

	var response *WriteByQueryResponse

	if _, err := FromJson(result, &response); err != nil {
		return nil, err
	}

	return response, nil
}

// Destroy executes a delete by query
func (esb *ElasticsearchBuilder) Destroy() (*WriteByQueryResponse, error) {
	ctx := context.Background()

	query := esb.client.DeleteByQuery(esb.index).ProceedOnVersionConflict().Query(esb.query())

	destroyResponse, err := query.Refresh("true").Do(ctx)

	if err != nil {
		return nil, err
	}

	result, err := ToJson(destroyResponse)

	if err != nil {
		return nil, err
	}

	var response *WriteByQueryResponse

	if _, err := FromJson(result, &response); err != nil {
		return nil, err
	}

	return response, nil
}

// Count retrieves the number of elements that match the query
func (esb *ElasticsearchBuilder) Count() (int64, error) {
	if err := esb.validateMustClauses(); err != nil {
		return 0, err
	}

	return esb.client.Count(esb.index).Query(esb.query()).Pretty(true).Do(context.Background())
}

// Cursor paginates based on searching after the last returned sortValues
func (esb *ElasticsearchBuilder) Cursor(offset int, sortValues []interface{}, items interface{}) ([]interface{}, error) {
	if offset == 0 || offset > LIMIT {
		return nil, errors.New("Offset must be greater than 0 and lesser or equal to 10000")
	}

	if esb.sorts == nil {
		return nil, errors.New("Please specify at least a sort field")
	}

	esb.Limit(offset)

	searchService, err := esb.build()

	if err != nil {
		return nil, err
	}

	if sortValues != nil {
		searchService.SearchAfter(sortValues...)
	}

	response, err := searchService.Do(context.Background())

	if err != nil {
		return nil, err
	}

	sortResponse, results, err := esb.processCursorResults(response.Hits.Hits)

	if err != nil {
		return nil, err
	}

	return sortResponse, json.Unmarshal([]byte(results), items)
}

func (esb *ElasticsearchBuilder) MinMax(field string, isDateField bool) (*MinMaxResponse, error) {
	rawQuery := `{
		"aggs": {
		  "min": {
			"min": {
			  "field": "` + field + `"
			}
		  },
		  "max": {
			"max": {
			  "field": "` + field + `"
			}
		  }
		}
	  }`

	result, err := esb.client.
		Search().
		Index(esb.index).
		Source(rawQuery).
		Size(0).
		Do(context.Background())

	if err != nil {
		return nil, err
	}

	return esb.parseMinMaxResponse(result.Aggregations, isDateField)
}

func (esb *ElasticsearchBuilder) RawQuery(rawQuery string) elastic.Query {
	return elastic.RawStringQuery(rawQuery)
}

func (esb *ElasticsearchBuilder) processCursorResults(hits []*elastic.SearchHit) ([]interface{}, string, error) {
	sources := []*json.RawMessage{}
	sortResponse := []interface{}{}
	chunkSize := (len(hits) + CONCURRENT_BATCH - 1) / CONCURRENT_BATCH
	chunkCount := int(math.Ceil(float64(len(hits)) / float64(chunkSize)))
	channels := make(chan map[int][]*json.RawMessage, chunkCount)

	for i := 0; i < len(hits); i += chunkSize {
		end := i + chunkSize

		if end >= len(hits) {
			end = len(hits)

			sortResponse = hits[len(hits)-1].Sort
		}

		go esb.processChunks(channels, hits[i:end], i)
	}

	sourceMaps := map[int][]*json.RawMessage{}

	for i := 0; i < chunkCount; i++ {
		for key, values := range <-channels {
			sourceMaps[key] = values
		}
	}

	for i := 0; i < chunkCount; i++ {
		sources = append(sources, sourceMaps[i]...)
	}

	results, err := ToJson(sources)

	return sortResponse, results, err
}

func (esb *ElasticsearchBuilder) processGetResults(hits []*elastic.SearchHit) []*json.RawMessage {
	sources := []*json.RawMessage{}

	if len(hits) == 0 {
		return sources
	}

	chunkSize := (len(hits) + CONCURRENT_BATCH - 1) / CONCURRENT_BATCH
	chunkCount := int(math.Ceil(float64(len(hits)) / float64(chunkSize)))
	channels := make(chan map[int][]*json.RawMessage, chunkCount)

	for i := 0; i < len(hits); i += chunkSize {
		end := i + chunkSize

		if end >= len(hits) {
			end = len(hits)
		}

		go esb.processChunks(channels, hits[i:end], i)
	}

	sourceMaps := map[int][]*json.RawMessage{}

	for i := 0; i < chunkCount; i++ {
		for key, values := range <-channels {
			sourceMaps[key] = values
		}
	}

	for i := 0; i < chunkCount; i++ {
		sources = append(sources, sourceMaps[i]...)
	}

	return sources
}

func (esb *ElasticsearchBuilder) processChunks(channels chan map[int][]*json.RawMessage, hits []*elastic.SearchHit, chunk int) {
	sources := []*json.RawMessage{}
	result := map[int][]*json.RawMessage{}

	for _, hit := range hits {
		sources = append(sources, &hit.Source)
	}

	result[chunk] = sources

	channels <- result
}

func (esb *ElasticsearchBuilder) processBulkRequest(batchClient *elastic.BulkService, num int) (*WriteResponse, error) {
	if batchClient.NumberOfActions() != num {
		return nil, errors.New("The number of actions does not match the number of arguments.")
	}

	batchResponse, err := batchClient.Do(context.Background())

	if err != nil {
		return nil, err
	}

	if batchClient.NumberOfActions() != 0 {
		return nil, errors.New("The number of actions send does not match the number of arguments.")
	}

	result, err := ToJson(batchResponse)

	if err != nil {
		return nil, err
	}

	var response *WriteResponse

	if _, err := FromJson(result, &response); err != nil {
		return nil, err
	}

	return response, nil
}

func (esb *ElasticsearchBuilder) processAggregations(aggregations elastic.Aggregations) (AggregationResponses, error) {
	aggregationResponse := make(AggregationResponses)

	for field, source := range aggregations {
		data, err := source.MarshalJSON()

		if err != nil {
			return nil, err
		}

		jsonParsed, err := gabs.ParseJSON(data)

		if err != nil {
			return nil, err
		}

		buckets, err := jsonParsed.Path("buckets").Children()

		if err != nil {
			return nil, err
		}

		docCountErrorUpperBound, _ := jsonParsed.Path("doc_count_error_upper_bound").Data().(float64)
		sumOtherDocCount, _ := jsonParsed.Path("sum_other_doc_count").Data().(float64)

		items, err := esb.processAggregationBuckets(buckets)

		if err != nil {
			return nil, err
		}

		aggregation := new(AggregationResponse)
		aggregation.DocCountErrorUpperBound = int(docCountErrorUpperBound)
		aggregation.SumOtherDocCount = int(sumOtherDocCount)

		if len(items) > 0 {
			aggregation.Buckets = items
		}

		aggregationResponse[field] = aggregation
	}

	return aggregationResponse, nil
}

func (esb *ElasticsearchBuilder) processAggregationBuckets(buckets []*gabs.Container) (AggregationBuckets, error) {
	items := AggregationBuckets{}

	for _, bucket := range buckets {
		aggregationBucket := new(AggregationBucket)
		subAggregations := AggregationResponses{}

		for _, field := range SliceRemove(0, esb.groupBy.GetFields()) {
			data, err := json.Marshal(bucket.Path(field).Data())

			if err != nil {
				return nil, err
			}

			var subAggregation *AggregationResponse

			if err := json.Unmarshal(data, &subAggregation); err != nil {
				return nil, err
			}

			subAggregations[field] = subAggregation
		}

		docCount, _ := bucket.Path("doc_count").Data().(float64)

		aggregationBucket.DocCount = int(docCount)
		aggregationBucket.Items = subAggregations
		aggregationBucket.Key = bucket.Path("key").Data()

		items = append(items, aggregationBucket)
	}

	return items, nil
}

func (esb *ElasticsearchBuilder) updateByQuery() (*elastic.UpdateByQueryService, error) {
	if err := esb.validateMustClauses(); err != nil {
		return nil, err
	}

	return esb.client.UpdateByQuery(esb.index).ProceedOnVersionConflict().Query(esb.query()), nil
}

func (esb *ElasticsearchBuilder) buildScript(params map[string]interface{}) *elastic.Script {
	script := ""

	for field, _ := range params {
		script = script + "ctx._source." + field + " = params." + field + "; "
	}

	return elastic.NewScript(script).Lang("painless").Params(params)
}

func (esb *ElasticsearchBuilder) build() (*elastic.SearchService, error) {
	query := esb.client.Search().Index(esb.index)

	if err := esb.validateMustClauses(); err != nil {
		return nil, err
	}

	query = query.Query(esb.query())

	if esb.sorts != nil {
		for _, sort := range esb.sorts {
			query = query.Sort(sort.GetField(), sort.GetOrder())
		}
	}

	if esb.limit != nil {
		if err := esb.validateLimit(); err != nil {
			return nil, err
		}

		query = query.Size(esb.limit.GetLimit())
	}

	if esb.from != nil {
		if err := esb.validateFrom(); err != nil {
			return nil, err
		}

		query = query.From(esb.from.GetFrom())
	}

	if esb.groupBy != nil {
		query = esb.processGroupBy(esb.groupBy.GetFields(), query)
	}

	return query, nil
}

func (esb *ElasticsearchBuilder) query() *elastic.BoolQuery {
	q := elastic.NewBoolQuery()

	wheres := make(chan []elastic.Query)
	notWheres := make(chan []elastic.Query)
	matches := make(chan []elastic.Query)
	notMatches := make(chan []elastic.Query)
	filters := make(chan []elastic.Query)

	go esb.processWheres(wheres, notWheres)
	go esb.processMatches(matches, notMatches)
	go esb.processFilters(filters)

	return q.Must(<-wheres...).
		MustNot(<-notWheres...).
		Must(<-matches...).
		MustNot(<-notMatches...).
		Filter(<-filters...)
}

func (esb *ElasticsearchBuilder) processWheres(wheres chan []elastic.Query, notWheres chan []elastic.Query) {
	var terms []elastic.Query
	var notTerms []elastic.Query

	for _, whereIn := range esb.whereIns {
		terms = append(terms, elastic.NewTermsQuery(whereIn.Field, whereIn.Values...))
	}

	for _, whereNotIn := range esb.whereNotIns {
		notTerms = append(notTerms, elastic.NewTermsQuery(whereNotIn.Field, whereNotIn.Values...))
	}

	for _, where := range esb.wheres {
		if where.GetOperand() == "=" {
			terms = append(terms, elastic.NewTermQuery(where.GetField(), where.GetValue()))
			continue
		}

		if where.GetOperand() == "<>" {
			notTerms = append(notTerms, elastic.NewTermQuery(where.GetField(), where.GetValue()))
			continue
		}

		if !where.IsString() || where.IsDate() {
			switch where.GetOperand() {
			case ">":
				terms = append(terms, elastic.NewRangeQuery(where.GetField()).Gt(where.GetValue()))
				break
			case "<":
				terms = append(terms, elastic.NewRangeQuery(where.GetField()).Lt(where.GetValue()))
				break
			case ">=":
				terms = append(terms, elastic.NewRangeQuery(where.GetField()).Gte(where.GetValue()))
				break
			case "<=":
				terms = append(terms, elastic.NewRangeQuery(where.GetField()).Lte(where.GetValue()))
				break
			}
		}
	}

	wheres <- terms
	notWheres <- notTerms
}

func (esb *ElasticsearchBuilder) processFilters(filters chan []elastic.Query) {
	var terms []elastic.Query

	for _, filterIn := range esb.filterIns {
		terms = append(terms, elastic.NewTermsQuery(filterIn.Field, filterIn.Values...))
	}

	for _, filter := range esb.filters {
		if filter.GetOperand() == "=" {
			terms = append(terms, elastic.NewTermQuery(filter.GetField(), filter.GetValue()))
			continue
		}

		if !filter.IsString() || filter.IsDate() {
			switch filter.GetOperand() {
			case ">":
				terms = append(terms, elastic.NewRangeQuery(filter.GetField()).Gt(filter.GetValue()))
				break
			case "<":
				terms = append(terms, elastic.NewRangeQuery(filter.GetField()).Lt(filter.GetValue()))
				break
			case ">=":
				terms = append(terms, elastic.NewRangeQuery(filter.GetField()).Gte(filter.GetValue()))
				break
			case "<=":
				terms = append(terms, elastic.NewRangeQuery(filter.GetField()).Lte(filter.GetValue()))
				break
			}
		}
	}

	filters <- terms
}

func (esb *ElasticsearchBuilder) processMatches(matches chan []elastic.Query, notMatches chan []elastic.Query) {
	var terms []elastic.Query
	var notTerms []elastic.Query

	for _, matchIn := range esb.matchIns {
		for _, value := range matchIn.Values {
			terms = append(terms, elastic.NewMatchQuery(matchIn.Field, value))
		}
	}

	for _, matchNotIn := range esb.matchNotIns {
		for _, value := range matchNotIn.Values {
			notTerms = append(notTerms, elastic.NewMatchQuery(matchNotIn.Field, value))
		}
	}

	for _, match := range esb.matches {
		if match.GetOperand() == "=" {
			terms = append(terms, elastic.NewMatchQuery(match.GetField(), match.GetValue()))
		}

		if match.GetOperand() == "<>" {
			notTerms = append(notTerms, elastic.NewMatchQuery(match.GetField(), match.GetValue()))
		}
	}

	matches <- terms
	notMatches <- notTerms
}

func (esb *ElasticsearchBuilder) processGroupBy(fields []string, query *elastic.SearchService) *elastic.SearchService {
	name := fields[0]

	aggr := elastic.NewTermsAggregation().Field(name)

	for _, field := range SliceRemove(0, fields) {
		aggr = aggr.SubAggregation(field, elastic.NewTermsAggregation().Field(field))
	}

	return query.Aggregation(name, aggr)
}

func (esb *ElasticsearchBuilder) parseMinMaxResponse(aggs elastic.Aggregations, isDateField bool) (*MinMaxResponse, error) {
	response := new(MinMaxResponse)

	check := "value"

	if isDateField {
		check = "value_as_string"
	}

	min, err := aggs["min"].MarshalJSON()

	if err != nil {
		return nil, err
	}

	max, err := aggs["max"].MarshalJSON()

	if err != nil {
		return nil, err
	}

	minContainer, err := gabs.ParseJSON(min)

	if err != nil {
		return nil, err
	}

	maxContainer, err := gabs.ParseJSON(max)

	if err != nil {
		return nil, err
	}

	val := maxContainer.S(check).Data()

	if val == nil {
		return nil, errors.New("Invalid conversion, could not find value")
	}

	response.Max = val

	val = minContainer.S(check).Data()

	if val == nil {
		return nil, errors.New("Invalid conversion, could not find value")
	}

	response.Min = val

	return response, nil
}
