package golastic

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Jeffail/gabs"
	elastic "github.com/alejandro-carstens/elasticfork"
)

// Builder represents the struct in charge of building
// and executing elasticsearch queries
type Builder struct {
	queryBuilder
	index    string
	client   *elastic.Client
	context  context.Context
	scroller *elastic.ScrollService
}

// Find retrieves an instance of a model for the specified Id from the corresponding elasticsearch index
func (b *Builder) Find(id string, item interface{}) error {
	response, err := b.client.Get().Index(b.index).Id(id).Do(b.context)

	if err != nil {
		return err
	}

	if response.Found == false {
		return errors.New("No document found for item with id:" + id)
	}

	data, err := response.Source.MarshalJSON()

	if err != nil {
		return err
	}

	return json.Unmarshal(data, item)
}

// InsertWithOverwrittenId allows to overwrite the of the given document on creation
func (b *Builder) InsertWithOverwrittenId(items map[string]interface{}) (*gabs.Container, error) {
	bulkClient := b.client.Bulk()

	for id, item := range items {
		bulkClient = bulkClient.Add(
			elastic.NewBulkIndexRequest().Index(b.index).Id(id).OpType("create").Doc(item),
		)
	}

	return b.processBulkRequest(bulkClient, len(items))
}

// Client returns an instance of *elastic.Client
func (b *Builder) Client() *elastic.Client {
	return b.client
}

// Insert inserts one or multiple documents into the corresponding elasticsearch index
func (b *Builder) Insert(items ...interface{}) (*gabs.Container, error) {
	bulkClient := b.client.Bulk()

	for _, item := range items {
		doc, err := toGabsContainer(item)

		if err != nil {
			return nil, err
		}

		id, valid := doc.S("id").Data().(string)

		if !valid {
			return nil, errors.New("id not specified in document.")
		}

		bulkClient = bulkClient.Add(
			elastic.NewBulkIndexRequest().Index(b.index).Id(id).OpType("create").Doc(item),
		)
	}

	return b.processBulkRequest(bulkClient, len(items))
}

// Delete deletes one or multiple documents by id from the corresponding elasticsearch index
func (b *Builder) Delete(ids ...string) (*gabs.Container, error) {
	batchClient := b.client.Bulk()

	for _, id := range ids {
		batchClient = batchClient.Add(
			elastic.NewBulkDeleteRequest().Index(b.index).Id(id),
		)
	}

	return b.processBulkRequest(batchClient, len(ids))
}

// Update updates one or multiple documents from the corresponding elasticsearch index
func (b *Builder) Update(items ...interface{}) (*gabs.Container, error) {
	batchClient := b.client.Bulk()

	for _, item := range items {
		doc, err := toGabsContainer(item)

		if err != nil {
			return nil, err
		}

		id, valid := doc.S("id").Data().(string)

		if !valid {
			return nil, errors.New("id not specified in document.")
		}

		batchClient = batchClient.Add(
			elastic.NewBulkUpdateRequest().Index(b.index).Id(id).Doc(item),
		)
	}

	return b.processBulkRequest(batchClient, len(items))
}

// Aggregate retrieves all the queries aggregations
func (b *Builder) Aggregate() (map[string]*AggregationResponse, error) {
	searchService, err := b.build()

	if err != nil {
		return nil, err
	}

	response, err := searchService.Do(b.context)

	if err != nil {
		return nil, err
	}

	if response.Aggregations == nil {
		return nil, errors.New("No aggregations returned")
	}

	return b.processAggregations(response.Aggregations)
}

// AggregateRaw returns raw aggregation results
func (b *Builder) AggregateRaw() (*gabs.Container, error) {
	searchService, err := b.build()

	if err != nil {
		return nil, err
	}

	response, err := searchService.Do(b.context)

	if err != nil {
		return nil, err
	}

	if response.Aggregations == nil {
		return nil, errors.New("No aggregations returned")
	}

	return toGabsContainer(response.Aggregations)
}

// Get executes the search query and retrieves the results
func (b *Builder) Get(items interface{}) error {
	searchService, err := b.build()

	if err != nil {
		return err
	}

	response, err := searchService.Do(b.context)

	if err != nil {
		return err
	}

	sources := b.processGetResults(response.Hits.Hits)

	results, err := toJson(sources)

	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(results), items)
}

// Execute executes an update by query
func (b *Builder) Execute(params map[string]interface{}) (*gabs.Container, error) {
	query, err := b.buildExecuteQuery(params)

	if err != nil {
		return nil, err
	}

	response, err := query.WaitForCompletion(true).Refresh("true").Do(b.context)

	if err != nil {
		return nil, err
	}

	return toGabsContainer(response)
}

// ExecuteAsync executes an update by query asynchronously
func (b *Builder) ExecuteAsync(scrollSize int, params map[string]interface{}) (*gabs.Container, error) {
	query, err := b.buildExecuteQuery(params)

	if err != nil {
		return nil, err
	}

	response, err := query.WaitForCompletion(false).DoAsync(b.context)

	if err != nil {
		return nil, err
	}

	return toGabsContainer(response)
}

// Destroy executes a delete by query
func (b *Builder) Destroy() (*gabs.Container, error) {
	response, err := b.client.
		DeleteByQuery(b.index).
		Refresh("true").
		ProceedOnVersionConflict().
		Query(b.query()).
		Do(b.context)

	if err != nil {
		return nil, err
	}

	return toGabsContainer(response)
}

// DestroyAsync executes a delete by query asynchronously
func (b *Builder) DestroyAsync() (*gabs.Container, error) {
	response, err := b.client.
		DeleteByQuery(b.index).
		ProceedOnVersionConflict().
		Query(b.query()).
		DoAsync(b.context)

	if err != nil {
		return nil, err
	}

	return toGabsContainer(response)
}

// RawQuery return an elastic raw query
func (b *Builder) RawQuery(query string) elastic.Query {
	return elastic.RawStringQuery(query)
}

// Count retrieves the number of elements that match the query
func (b *Builder) Count() (int64, error) {
	if err := b.validateMustClauses(); err != nil {
		return 0, err
	}

	return b.client.Count(b.index).Query(b.query()).Pretty(true).Do(b.context)
}

// Cursor paginates based on searching after the last returned sortValues
func (b *Builder) Cursor(offset int, sortValues []interface{}, items interface{}) ([]interface{}, error) {
	if offset == 0 || offset > LIMIT {
		return nil, errors.New("Offset must be greater than 0 and lesser or equal to 10000")
	}

	if b.sorts == nil {
		return nil, errors.New("Please specify at least a sort field")
	}

	b.Limit(offset)

	searchService, err := b.build()

	if err != nil {
		return nil, err
	}

	if sortValues != nil {
		searchService.SearchAfter(sortValues...)
	}

	response, err := searchService.Do(b.context)

	if err != nil {
		return nil, err
	}

	sortResponse, results, err := b.processCursorResults(response.Hits.Hits)

	if err != nil {
		return nil, err
	}

	return sortResponse, json.Unmarshal([]byte(results), items)
}

// MinMax returns the minimum and maximum values for a given field on an index
func (b *Builder) MinMax(field string, isDateField bool) (*MinMaxResponse, error) {
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

	result, err := b.client.Search().Index(b.index).Source(rawQuery).Size(0).Do(b.context)

	if err != nil {
		return nil, err
	}

	return b.parseMinMaxResponse(result.Aggregations, isDateField)
}

// GetTask retrieves a task given a taskId
func (b *Builder) GetTask(taskId string, waitForCompletion bool) (*gabs.Container, error) {
	response, err := b.client.TasksGetTask().TaskId(taskId).WaitForCompletion(waitForCompletion).Do(b.context)

	if err != nil {
		return nil, err
	}

	return toGabsContainer(response)
}

// CancelTask cancels a task provided a taskId
func (b *Builder) CancelTask(taskId string) (*gabs.Container, error) {
	response, err := b.client.TasksCancel().TaskId(taskId).Do(b.context)

	if err != nil {
		return nil, err
	}

	return toGabsContainer(response)
}

// InitScroller initializes the scroller
func (b *Builder) InitScroller(size int, scroll string) *Builder {
	b.scroller = b.client.Scroll(b.index).Query(b.query()).Size(size).Scroll(scroll)

	return b
}

// InitSlicedScroller boots a sliced scroller
func (b *Builder) InitSlicedScroller(id, max, size int, scroll string) *Builder {
	query := b.query()
	sliceQuery := elastic.NewSliceQuery().Id(id).Max(max)

	b.scroller = b.client.Scroll(b.index).
		Slice(sliceQuery).
		Query(query).
		Size(size).
		Scroll(scroll)

	return b
}

// Scroll executes the scrolling
func (b *Builder) Scroll() (*gabs.Container, error) {
	if b.scroller == nil {
		return nil, errors.New("scroller is empty")
	}

	results, err := b.scroller.Do(b.context)

	if err != nil {
		return nil, err
	}

	return toGabsContainer(results)
}

// ClearScroll cancel's the current scroll operation
func (b *Builder) ClearScroll() error {
	if b.scroller == nil {
		return errors.New("scroller is empty")
	}

	return b.scroller.Clear(b.context)
}

func (b *Builder) processCursorResults(hits []*elastic.SearchHit) ([]interface{}, string, error) {
	sources := []*json.RawMessage{}
	sortResponse := []interface{}{}

	chunkSize := calculateChunkSize(len(hits))
	chunkCount := calculateChunkCount(len(hits), chunkSize)
	channels := make(chan map[int][]*json.RawMessage, chunkCount)
	counter := 0

	for i := 0; i < len(hits); i += chunkSize {
		end := i + chunkSize

		if end >= len(hits) {
			end = len(hits)

			sortResponse = hits[len(hits)-1].Sort
		}

		go b.processChunks(channels, hits[i:end], counter)

		counter++
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

	close(channels)

	results, err := toJson(sources)

	return sortResponse, results, err
}

func (b *Builder) processGetResults(hits []*elastic.SearchHit) []*json.RawMessage {
	sources := []*json.RawMessage{}

	if len(hits) == 0 {
		return sources
	}

	chunkSize := calculateChunkSize(len(hits))
	chunkCount := calculateChunkCount(len(hits), chunkSize)
	channels := make(chan map[int][]*json.RawMessage, chunkCount)
	counter := 0

	for i := 0; i < len(hits); i += chunkSize {
		end := i + chunkSize

		if end >= len(hits) {
			end = len(hits)
		}

		go b.processChunks(channels, hits[i:end], counter)

		counter++
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

	close(channels)

	return sources
}

func (b *Builder) processChunks(channels chan map[int][]*json.RawMessage, hits []*elastic.SearchHit, chunk int) {
	sources := []*json.RawMessage{}
	result := map[int][]*json.RawMessage{}

	for _, hit := range hits {
		sources = append(sources, &hit.Source)
	}

	result[chunk] = sources

	channels <- result
}

func (b *Builder) processBulkRequest(batchClient *elastic.BulkService, num int) (*gabs.Container, error) {
	if batchClient.NumberOfActions() != num {
		return nil, errors.New("The number of actions does not match the number of arguments.")
	}

	response, err := batchClient.Do(b.context)

	if err != nil {
		return nil, err
	}

	if batchClient.NumberOfActions() != 0 {
		return nil, errors.New("The number of actions send does not match the number of arguments.")
	}

	return toGabsContainer(response)
}

func (b *Builder) processAggregations(aggregations elastic.Aggregations) (AggregationResponses, error) {
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

		items, err := b.processAggregationBuckets(buckets)

		if err != nil {
			return nil, err
		}

		docCountErrorUpperBound, _ := jsonParsed.Path("doc_count_error_upper_bound").Data().(float64)
		sumOtherDocCount, _ := jsonParsed.Path("sum_other_doc_count").Data().(float64)

		aggregationResponse[field] = &AggregationResponse{
			DocCountErrorUpperBound: int(docCountErrorUpperBound),
			SumOtherDocCount:        int(sumOtherDocCount),
			Buckets:                 items,
		}
	}

	return aggregationResponse, nil
}

func (b *Builder) processAggregationBuckets(buckets []*gabs.Container) (aggregationBuckets, error) {
	items := aggregationBuckets{}

	for _, bucket := range buckets {
		subAggregations := AggregationResponses{}

		for _, field := range sliceRemove(0, b.groupBy.Fields) {
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

		items = append(items, &AggregationBucket{
			DocCount: int(docCount),
			Items:    subAggregations,
			Key:      bucket.Path("key").Data(),
		})
	}

	return items, nil
}

func (b *Builder) buildExecuteQuery(params map[string]interface{}) (*elastic.UpdateByQueryService, error) {
	query, err := b.updateByQuery()

	if err != nil {
		return nil, err
	}

	script := ""

	for field := range params {
		script = script + "ctx._source." + field + " = params." + field + "; "
	}

	return query.Script(elastic.NewScript(script).Lang("painless").Params(params)).ProceedOnVersionConflict(), nil
}

func (b *Builder) updateByQuery() (*elastic.UpdateByQueryService, error) {
	if err := b.validateMustClauses(); err != nil {
		return nil, err
	}

	return b.client.UpdateByQuery(b.index).ProceedOnVersionConflict().Query(b.query()), nil
}

func (b *Builder) build() (*elastic.SearchService, error) {
	query := b.client.Search().Index(b.index)

	if err := b.validateMustClauses(); err != nil {
		return nil, err
	}

	query = query.Query(b.query())

	if b.sorts != nil {
		for _, sort := range b.sorts {
			query = query.Sort(sort.Field, sort.Order)
		}
	}

	if b.limit != nil {
		if err := b.validateLimit(); err != nil {
			return nil, err
		}

		query = query.Size(b.limit.Limit)
	}

	if b.nestedSort != nil {
		if err := b.nestedSort.validate(); err != nil {
			return nil, err
		}

		nestedSort := elastic.NewNestedSort(b.nestedSort.Path)

		query = query.SortBy(elastic.NewFieldSort(b.nestedSort.Field).Nested(nestedSort).Order(b.nestedSort.Order))
	}

	if b.from != nil {
		if err := b.validateFrom(); err != nil {
			return nil, err
		}

		query = query.From(b.from.From)
	}

	if b.groupBy != nil {
		query = b.processGroupBy(b.groupBy.Fields, query)
	}

	if b.stats != nil {
		query = b.processStatsAggregations(b.stats.Fields, query)
	}

	return query, nil
}

func (b *Builder) query() *elastic.BoolQuery {
	wheres := make(chan []elastic.Query)
	notWheres := make(chan []elastic.Query)
	matches := make(chan []elastic.Query)
	notMatches := make(chan []elastic.Query)
	matchPhrases := make(chan []elastic.Query)
	notMatchPhrases := make(chan []elastic.Query)
	filters := make(chan []elastic.Query)
	nestedQueries := make(chan []elastic.Query)

	go func() {
		terms, notTerms := processWheres(b.wheres, b.whereIns, b.whereNotIns)

		wheres <- terms
		notWheres <- notTerms
	}()

	go func() {
		filters <- processFilters(b.filters, b.filterIns)
	}()

	go func() {
		terms, notTerms := processMatches(b.matches, b.matchIns, b.matchNotIns)

		matches <- terms
		notMatches <- notTerms
	}()

	go func() {
		terms, notTerms := processMatchPhrases(b.matchPhrases, b.matchPhraseIns, b.matchPhraseNotIns)

		matchPhrases <- terms
		notMatchPhrases <- notTerms
	}()

	go b.processNestedQueries(nestedQueries)

	query := elastic.NewBoolQuery().
		Must(<-wheres...).
		MustNot(<-notWheres...).
		Must(<-matches...).
		MustNot(<-notMatches...).
		Filter(<-filters...).
		Must(<-matchPhrases...).
		MustNot(<-notMatchPhrases...).
		Must(<-nestedQueries...)

	close(wheres)
	close(notWheres)
	close(matches)
	close(notMatches)
	close(matchPhrases)
	close(notMatchPhrases)
	close(filters)
	close(nestedQueries)

	return query
}

func (b *Builder) processNestedQueries(nestedQueries chan []elastic.Query) {
	var queries []elastic.Query

	for path, nested := range b.nested {
		filters := processFilters(nested.filters, nested.filterIns)
		terms, notTerms := processWheres(nested.wheres, nested.whereIns, nested.whereNotIns)
		matches, notMatches := processMatches(nested.matches, nil, nil)
		matchPhrases, notMatchPhrases := processMatchPhrases(nested.matchPhrases, nil, nil)

		query := elastic.NewBoolQuery().
			Must(terms...).
			MustNot(notTerms...).
			Filter(filters...).
			Must(matches...).
			MustNot(notMatches...).
			Must(matchPhrases...).
			MustNot(notMatchPhrases...)

		queries = append(queries, elastic.NewNestedQuery(path, query))
	}

	nestedQueries <- queries
}

func (b *Builder) processStatsAggregations(fields []string, query *elastic.SearchService) *elastic.SearchService {
	name := fields[0]

	aggr := elastic.NewExtendedStatsAggregation().Field(name)

	for _, field := range sliceRemove(0, fields) {
		aggr = aggr.SubAggregation(field, elastic.NewExtendedStatsAggregation().Field(field))
	}

	return query.Aggregation(name, aggr)
}

func (b *Builder) processGroupBy(fields []string, query *elastic.SearchService) *elastic.SearchService {
	name := fields[0]

	aggr := elastic.NewTermsAggregation().Field(name)

	for _, field := range sliceRemove(0, fields) {
		aggr = aggr.SubAggregation(field, elastic.NewTermsAggregation().Field(field))
	}

	return query.Aggregation(name, aggr)
}

func (b *Builder) parseMinMaxResponse(aggs elastic.Aggregations, isDateField bool) (*MinMaxResponse, error) {
	response := &MinMaxResponse{}

	check := VALUE

	if isDateField {
		check = VALUE_AS_STRING
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

func processWheres(
	wheres []*where,
	whereIns []*whereIn,
	whereNotIns []*whereNotIn,
) (terms []elastic.Query, notTerms []elastic.Query) {
	for _, whereIn := range whereIns {
		terms = append(terms, elastic.NewTermsQuery(whereIn.Field, whereIn.Values...))
	}

	for _, whereNotIn := range whereNotIns {
		notTerms = append(notTerms, elastic.NewTermsQuery(whereNotIn.Field, whereNotIn.Values...))
	}

	for _, where := range wheres {
		if where.Operand == "=" {
			terms = append(terms, elastic.NewTermQuery(where.Field, where.Value))
			continue
		}

		if where.Operand == "<>" {
			notTerms = append(notTerms, elastic.NewTermQuery(where.Field, where.Value))
			continue
		}

		if !where.isString() || where.isDate() {
			switch where.Operand {
			case ">":
				terms = append(terms, elastic.NewRangeQuery(where.Field).Gt(where.Value))
				break
			case "<":
				terms = append(terms, elastic.NewRangeQuery(where.Field).Lt(where.Value))
				break
			case ">=":
				terms = append(terms, elastic.NewRangeQuery(where.Field).Gte(where.Value))
				break
			case "<=":
				terms = append(terms, elastic.NewRangeQuery(where.Field).Lte(where.Value))
				break
			}
		}
	}

	return terms, notTerms
}

func processFilters(filters []*filter, filterIns []*filterIn) (terms []elastic.Query) {
	for _, filterIn := range filterIns {
		terms = append(terms, elastic.NewTermsQuery(filterIn.Field, filterIn.Values...))
	}

	for _, filter := range filters {
		if filter.Operand == "=" {
			terms = append(terms, elastic.NewTermQuery(filter.Field, filter.Value))
			continue
		}

		if !filter.isString() || filter.isDate() {
			switch filter.Operand {
			case ">":
				terms = append(terms, elastic.NewRangeQuery(filter.Field).Gt(filter.Value))
				break
			case "<":
				terms = append(terms, elastic.NewRangeQuery(filter.Field).Lt(filter.Value))
				break
			case ">=":
				terms = append(terms, elastic.NewRangeQuery(filter.Field).Gte(filter.Value))
				break
			case "<=":
				terms = append(terms, elastic.NewRangeQuery(filter.Field).Lte(filter.Value))
				break
			}
		}
	}

	return terms
}

func processMatches(
	matches []*match,
	matchIns []*matchIn,
	matchNotIns []*matchNotIn,
) (terms []elastic.Query, notTerms []elastic.Query) {
	for _, matchIn := range matchIns {
		for _, value := range matchIn.Values {
			terms = append(terms, elastic.NewMatchQuery(matchIn.Field, value))
		}
	}

	for _, matchNotIn := range matchNotIns {
		for _, value := range matchNotIn.Values {
			notTerms = append(notTerms, elastic.NewMatchQuery(matchNotIn.Field, value))
		}
	}

	for _, match := range matches {
		if match.Operand == "=" {
			terms = append(terms, elastic.NewMatchQuery(match.Field, match.Value))
		}

		if match.Operand == "<>" {
			notTerms = append(notTerms, elastic.NewMatchQuery(match.Field, match.Value))
		}
	}

	return terms, notTerms
}

func processMatchPhrases(
	matchPhrases []*matchPhrase,
	matchPhraseIns []*matchPhraseIn,
	matchPhraseNotIns []*matchPhraseNotIn,
) (terms []elastic.Query, notTerms []elastic.Query) {
	for _, matchPhraseIn := range matchPhraseIns {
		for _, value := range matchPhraseIn.Values {
			terms = append(terms, elastic.NewMatchPhraseQuery(matchPhraseIn.Field, value))
		}
	}

	for _, matchPhraseNotIn := range matchPhraseNotIns {
		for _, value := range matchPhraseNotIn.Values {
			notTerms = append(notTerms, elastic.NewMatchPhraseQuery(matchPhraseNotIn.Field, value))
		}
	}

	for _, matchPhrase := range matchPhrases {
		if matchPhrase.Operand == "=" {
			terms = append(terms, elastic.NewMatchPhraseQuery(matchPhrase.Field, matchPhrase.Value))
		}

		if matchPhrase.Operand == "<>" {
			notTerms = append(notTerms, elastic.NewMatchPhraseQuery(matchPhrase.Field, matchPhrase.Value))
		}
	}

	return terms, notTerms
}
