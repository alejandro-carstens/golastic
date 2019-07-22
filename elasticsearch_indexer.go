package golastic

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Jeffail/gabs"
	elastic "github.com/alejandro-carstens/elasticfork"
)

type IndexOptions struct {
	WaitForCompletion  bool
	IgnoreUnavailable  bool
	IncludeGlobalState bool
	Partial            bool
	IncludeAliases     bool
	Timeout            string
	RenamePattern      string
	RenameReplacement  string
	Indices            []string
	IndexSettings      map[string]interface{}
}

type ElasticsearchIndexer struct {
	ElasticsearchClient
	options *IndexOptions
}

// SetOptions sets the index options for the action to be performed
func (esi *ElasticsearchIndexer) SetOptions(options *IndexOptions) {
	esi.options = options
}

// Exists checks if a given index exists on ElasticSearch
func (esi *ElasticsearchIndexer) Exists(name string) (bool, error) {
	return esi.client.IndexExists(name).Do(context.Background())
}

// CreateIndex creates and ElasticSearch index
func (esi *ElasticsearchIndexer) CreateIndex(name string, schema string) error {
	ctx := context.Background()

	service := esi.client.CreateIndex(name)

	if esi.options != nil && len(esi.options.Timeout) > 0 {
		service.Timeout(esi.options.Timeout)
	}

	createIndex, err := service.BodyString(schema).Do(ctx)

	if err != nil {
		return err
	}

	if !createIndex.Acknowledged {
		return errors.New("The index was not acknowledged.")
	}

	return nil
}

// DeleteIndex deletes an ElasticSearch Index
func (esi *ElasticsearchIndexer) DeleteIndex(name string) error {
	ctx := context.Background()

	service := esi.client.DeleteIndex(name)

	if esi.options != nil && len(esi.options.Timeout) > 0 {
		service.Timeout(esi.options.Timeout)
	}

	deleteIndex, err := service.Do(ctx)

	if err != nil {
		return err
	}

	if !deleteIndex.Acknowledged {
		return errors.New("The index deletion was not acknowledged.")
	}

	return nil
}

// ListIndices lists all open inidces on an elsasticsearch cluster
func (esi *ElasticsearchIndexer) ListIndices() ([]string, error) {
	return esi.client.IndexNames()
}

// ListAllIndices lists all indices on and elasticsearch cluster
func (esi *ElasticsearchIndexer) ListAllIndices() ([]string, error) {
	ctx := context.Background()

	catIndicesResponse, err := esi.client.CatIndices().Columns("index").Do(ctx)

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(catIndicesResponse)

	if err != nil {
		return nil, err
	}

	container, err := gabs.ParseJSON(b)

	if err != nil {
		return nil, err
	}

	indices := []string{}

	children, err := container.Children()

	if err != nil {
		return nil, err
	}

	for _, child := range children {
		index, valid := child.S("index").Data().(string)

		if !valid {
			return nil, errors.New("Invalid type for index field on index cat response")
		}

		indices = append(indices, index)
	}

	return indices, nil
}

// Settings gets the index settings for the specified indices
func (esi *ElasticsearchIndexer) Settings(names ...string) (map[string]*gabs.Container, error) {
	ctx := context.Background()

	indicesSettings, err := esi.client.IndexGetSettings(names...).Do(ctx)

	if err != nil {
		return nil, err
	}

	settings := map[string]*gabs.Container{}

	for key, value := range indicesSettings {
		b, err := json.Marshal(value.Settings)

		if err != nil {
			return nil, err
		}

		container, err := gabs.ParseJSON(b)

		if err != nil {
			return nil, err
		}

		settings[key] = container
	}

	return settings, nil
}

func (esi *ElasticsearchIndexer) GetTask(taskId string) (*gabs.Container, error) {
	taskResponse, err := esi.client.TasksGetTask().TaskId(taskId).Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(taskResponse)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func (esi *ElasticsearchIndexer) GetClusterHealth(indices ...string) (*gabs.Container, error) {
	clusterHealthResponse, err := esi.client.ClusterHealth().Index(indices...).Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(clusterHealthResponse)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func (esi *ElasticsearchIndexer) GetIndices(indices ...string) (*gabs.Container, error) {
	indexGetResponse, err := esi.client.IndexGet(indices...).Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(indexGetResponse)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func (esi *ElasticsearchIndexer) PutSettings(body string, indices ...string) (*gabs.Container, error) {
	service := esi.client.IndexPutSettings(indices...).BodyString(body)

	if esi.options != nil && len(esi.options.Timeout) > 0 {
		service.MasterTimeout(esi.options.Timeout)
	}

	putSettingsResponse, err := service.Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(putSettingsResponse)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func (esi *ElasticsearchIndexer) CreateRepository(repository string, repoType string, verify bool, settings map[string]interface{}) (*gabs.Container, error) {
	service := esi.client.SnapshotCreateRepository(repository).Type(repoType).Verify(verify).Settings(settings)

	if esi.options != nil && len(esi.options.Timeout) > 0 {
		service.MasterTimeout(esi.options.Timeout)
	}

	response, err := service.Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(response)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func (esi *ElasticsearchIndexer) DeleteRepositories(respositories ...string) (*gabs.Container, error) {
	service := esi.client.SnapshotDeleteRepository(respositories...)

	if esi.options != nil && len(esi.options.Timeout) > 0 {
		service.MasterTimeout(esi.options.Timeout)
	}

	response, err := service.Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(response)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func (esi *ElasticsearchIndexer) Snapshot(repository string, snapshot string, indices ...string) (*gabs.Container, error) {
	service := esi.client.SnapshotCreate(repository, snapshot)

	if esi.options != nil {
		service.WaitForCompletion(esi.options.WaitForCompletion)
	}

	if esi.options != nil && len(esi.options.Timeout) > 0 {
		service.MasterTimeout(esi.options.Timeout)
	}

	body := map[string]interface{}{
		"ignore_unavailable":   esi.options.IgnoreUnavailable,
		"include_global_state": esi.options.IncludeGlobalState,
		"partial":              esi.options.Partial,
		"indices":              strings.Join(indices, ","),
	}

	b, err := json.Marshal(body)

	if err != nil {
		return nil, err
	}

	bodyJson, err := gabs.ParseJSON(b)

	if err != nil {
		return nil, err
	}

	snapshotResponse, err := service.BodyString(bodyJson.String()).Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err = json.Marshal(snapshotResponse)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func (esi *ElasticsearchIndexer) GetSnapshots(respository string, snapshot string) (*gabs.Container, error) {
	service := esi.client.SnapshotGet(respository)

	if snapshot != "*" && len(snapshot) > 0 {
		service.Snapshot(snapshot)
	}

	if esi.options != nil && len(esi.options.Timeout) > 0 {
		service.MasterTimeout(esi.options.Timeout)
	}

	if esi.options != nil && esi.options.IgnoreUnavailable {
		service.IgnoreUnavailable(esi.options.IgnoreUnavailable)
	}

	response, err := service.Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(response)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func (esi *ElasticsearchIndexer) ListSnapshots(repository string) ([]string, error) {
	response, err := esi.GetSnapshots(repository, "*")

	if err != nil {
		return nil, err
	}

	children, err := response.S("snapshots").Children()

	if err != nil {
		return nil, err
	}

	list := []string{}

	for _, child := range children {
		element, valid := child.Search("snapshot").Data().(string)

		if !valid {
			return nil, errors.New("Could not retrieve snapshot name")
		}

		list = append(list, element)
	}

	return list, nil
}

func (esi *ElasticsearchIndexer) DeleteSnapshot(repository string, name string) (*gabs.Container, error) {
	response, err := esi.client.SnapshotDelete(repository, name).Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(response)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func (esi *ElasticsearchIndexer) SnapshotRestore(repository string, snapshot string) (*gabs.Container, error) {
	service := esi.client.SnapshotRestore(repository, snapshot)

	if esi.options != nil && len(esi.options.Timeout) > 0 {
		service.MasterTimeout(esi.options.Timeout)
	}

	if esi.options != nil && len(esi.options.Indices) > 0 {
		service.Indices(esi.options.Indices...)
	}

	if esi.options != nil && len(esi.options.IndexSettings) > 0 {
		service.IndexSettings(esi.options.IndexSettings)
	}

	if esi.options != nil && len(esi.options.RenamePattern) > 0 {
		service.RenamePattern(esi.options.RenamePattern)
	}

	if esi.options != nil && len(esi.options.RenameReplacement) > 0 {
		service.RenameReplacement(esi.options.RenameReplacement)
	}

	if esi.options != nil {
		service.WaitForCompletion(esi.options.WaitForCompletion).
			IncludeAliases(esi.options.IncludeAliases).Partial(esi.options.Partial).
			IncludeGlobalState(esi.options.IncludeGlobalState)
	}

	response, err := service.Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(response)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func (esi *ElasticsearchIndexer) Recovery(indices ...string) (map[string]*gabs.Container, error) {
	response, err := esi.client.IndicesRecovery().Human(true).Indices(indices...).Do(context.Background())

	if err != nil {
		return nil, err
	}

	indicesRecoveryResponse := map[string]*gabs.Container{}

	for index, recovery := range response {
		b, err := json.Marshal(recovery)

		if err != nil {
			return nil, err
		}

		container, err := gabs.ParseJSON(b)

		if err != nil {
			return nil, err
		}

		indicesRecoveryResponse[index] = container
	}

	return indicesRecoveryResponse, nil
}

// Close closes an elasticsearch index
func (esi *ElasticsearchIndexer) Close(name string) (*gabs.Container, error) {
	service := esi.client.CloseIndex(name)

	if esi.options != nil && len(esi.options.Timeout) > 0 {
		service.MasterTimeout(esi.options.Timeout)
	}

	closeResponse, err := service.Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(closeResponse)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

// Close closes an elasticsearch index
func (esi *ElasticsearchIndexer) Open(name string) (*gabs.Container, error) {
	service := esi.client.OpenIndex(name)

	if esi.options != nil && len(esi.options.Timeout) > 0 {
		service.MasterTimeout(esi.options.Timeout)
	}

	closeResponse, err := service.Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(closeResponse)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

// IndexCat retrieves information assocaited to the given index
func (esi *ElasticsearchIndexer) IndexCat(name string) (*gabs.Container, error) {
	ctx := context.Background()

	catIndicesResponse, err := esi.client.CatIndices().Index(name).Columns(esi.columns()...).Do(ctx)

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(catIndicesResponse)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

// AliasesCat retrives information assocaited to all current index aliases
func (esi *ElasticsearchIndexer) AliasesCat() ([]*CatAliasesResponse, error) {
	catAliasesResponse, err := esi.client.CatAliases().Columns("*").Do(context.Background())

	if err != nil {
		return nil, err
	}

	response := []*CatAliasesResponse{}

	for _, row := range catAliasesResponse {
		catAliasesResponse := new(CatAliasesResponse)

		catAliasesResponse.Index = row.Index
		catAliasesResponse.Alias = row.Alias
		catAliasesResponse.Filter = row.Filter
		catAliasesResponse.RoutingIndex = row.RoutingIndex
		catAliasesResponse.RoutingSearch = row.RoutingSearch

		response = append(response, catAliasesResponse)
	}

	return response, nil
}

// AddAlias adds an alias to a given elasticsearch index
func (esi *ElasticsearchIndexer) AddAlias(indexName string, aliasName string) (*gabs.Container, error) {
	aliasResponse, err := elastic.NewAliasService(esi.client).Add(indexName, aliasName).Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(aliasResponse)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func (esi *ElasticsearchIndexer) AddAliasByAction(aliasAction *elastic.AliasAddAction) (*gabs.Container, error) {
	aliasResponse, err := elastic.NewAliasService(esi.client).Action(aliasAction).Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(aliasResponse)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func (esi *ElasticsearchIndexer) RemoveIndexFromAlias(index string, alias string) (*gabs.Container, error) {
	aliasResponse, err := elastic.NewAliasService(esi.client).Remove(index, alias).Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(aliasResponse)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func (esi *ElasticsearchIndexer) AliasAddAction(alias string) *elastic.AliasAddAction {
	return elastic.NewAliasAddAction(alias)
}

// IndexStats retrieves the statistics for the given indices
func (esi *ElasticsearchIndexer) IndexStats(indices ...string) (map[string]*gabs.Container, error) {
	indexStatsResponse, err := esi.client.IndexStats(indices...).Do(context.Background())

	if err != nil {
		return nil, err
	}

	mapContainer := map[string]*gabs.Container{}

	for index, indexStats := range indexStatsResponse.Indices {
		b, err := json.Marshal(indexStats)

		if err != nil {
			return nil, err
		}

		container, err := gabs.ParseJSON(b)

		if err != nil {
			return nil, err
		}

		mapContainer[index] = container
	}

	return mapContainer, nil
}

func (esi *ElasticsearchIndexer) Rollover(alias, newIndex, maxAge, maxSize string, maxDocs int64, settings map[string]interface{}) (*gabs.Container, error) {
	service := esi.client.RolloverIndex(alias)

	if esi.options != nil && len(esi.options.Timeout) > 0 {
		service.MasterTimeout(esi.options.Timeout)
	}

	if len(newIndex) > 0 {
		service.NewIndex(newIndex)
	}

	if len(maxAge) > 0 {
		service.AddCondition("max_age", maxAge)
	}

	if len(maxSize) > 0 {
		service.AddCondition("max_size", maxSize)
	}

	if maxDocs > int64(0) {
		service.AddCondition("max_docs", maxDocs)
	}

	if settings != nil {
		service.Settings(settings)
	}

	response, err := service.Do(context.Background())

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(response)

	if err != nil {
		return nil, err
	}

	return gabs.ParseJSON(b)
}

func (esi *ElasticsearchIndexer) columns() []string {
	return []string{
		"health",
		"status",
		"index",
		"uuid",
		"pri",
		"rep",
		"docs.count",
		"docs.deleted",
		"creation.date",
		"creation.date.string",
		"store.size",
		"pri.store.size",
		"completion.size",
		"pri.completion.size",
		"fielddata.memory_size",
		"pri.fielddata.memory_size",
		"fielddata.evictions",
		"pri.fielddata.evictions",
		"query_cache.memory_size",
		"pri.query_cache.memory_size",
		"query_cache.evictions",
		"pri.query_cache.evictions",
		"request_cache.memory_size",
		"pri.request_cache.memory_size",
		"request_cache.evictions",
		"pri.request_cache.evictions",
		"request_cache.hit_count",
		"pri.request_cache.hit_count",
		"request_cache.miss_count",
		"pri.request_cache.miss_count",
		"flush.total_time",
		"pri.flush.total_time",
		"get.current",
		"pri.get.current",
		"get.time",
		"pri.get.time",
		"get.total",
		"pri.get.total",
		"get.exists_time",
		"pri.get.exists_time",
		"get.exists_total",
		"pri.get.exists_total",
		"get.missing_time",
		"pri.get.missing_time",
		"get.missing_total",
		"pri.get.missing_total",
		"indexing.delete_current",
		"pri.indexing.delete_current",
		"indexing.delete_time",
		"pri.indexing.delete_time",
		"indexing.delete_total",
		"pri.indexing.delete_total",
		"indexing.index_current",
		"pri.indexing.index_current",
		"indexing.index_time",
		"pri.indexing.index_time",
		"indexing.index_total",
		"pri.indexing.index_total",
		"indexing.index_failed",
		"pri.indexing.index_failed",
		"merges.current",
		"pri.merges.current",
		"merges.current_docs",
		"pri.merges.current_docs",
		"merges.current_size",
		"pri.merges.current_size",
		"merges.total",
		"pri.merges.total",
		"merges.total_docs",
		"pri.merges.total_docs",
		"merges.total_size",
		"pri.merges.total_size",
		"merges.total_time",
		"pri.merges.total_time",
		"refresh.total",
		"pri.refresh.total",
		"refresh.time",
		"pri.refresh.time",
		"refresh.listeners",
		"pri.refresh.listeners",
		"search.fetch_current",
		"pri.search.fetch_current",
		"search.fetch_time",
		"pri.search.fetch_time",
		"search.fetch_total",
		"pri.search.fetch_total",
		"search.open_contexts",
		"pri.search.open_contexts",
		"search.query_current",
		"pri.search.query_current",
		"search.query_time",
		"pri.search.query_time",
		"search.query_total",
		"pri.search.query_total",
		"search.scroll_current",
		"pri.search.scroll_current",
		"search.scroll_time",
		"pri.search.scroll_time",
		"search.scroll_total",
		"pri.search.scroll_total",
		"segments.count",
		"pri.segments.count",
		"segments.memory",
		"pri.segments.memory",
		"segments.index_writer_memory",
		"pri.segments.index_writer_memory",
		"segments.version_map_memory",
		"pri.segments.version_map_memory",
		"segments.fixed_bitset_memory",
		"pri.segments.fixed_bitset_memory",
		"warmer.current",
		"pri.warmer.current",
		"warmer.total",
		"pri.warmer.total",
		"warmer.total_time",
		"pri.warmer.total_time",
		"suggest.current",
		"pri.suggest.current",
		"suggest.time",
		"pri.suggest.time",
		"suggest.total",
		"pri.suggest.total",
		"memory.total",
		"pri.memory.total",
	}
}
