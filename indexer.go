package golastic

import (
	"context"
	"errors"
	"strings"

	"github.com/Jeffail/gabs"
	elastic "github.com/alejandro-carstens/elasticfork"
)

// IndexOptions are options that can be passed as elasticsearch parameters
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

// Indexer represents a struct that performs different
// actions on elasticsearch indices
type Indexer struct {
	options *IndexOptions
	client  *elastic.Client
	context context.Context
}

// SetOptions sets the index options for the action to be performed
func (i *Indexer) SetOptions(options *IndexOptions) {
	i.options = options
}

// Exists checks if a given index exists on ElasticSearch
func (i *Indexer) Exists(name string) (bool, error) {
	return i.client.IndexExists(name).Do(i.context)
}

// CreateIndex creates and ElasticSearch index
func (i *Indexer) CreateIndex(name string, schema string) error {
	service := i.client.CreateIndex(name)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.Timeout(i.options.Timeout)
	}

	createIndex, err := service.BodyString(schema).Do(i.context)

	if err != nil {
		return err
	}

	if !createIndex.Acknowledged {
		return errors.New("The index was not acknowledged.")
	}

	return nil
}

// DeleteIndex deletes an ElasticSearch Index
func (i *Indexer) DeleteIndex(name string) error {
	service := i.client.DeleteIndex(name)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.Timeout(i.options.Timeout)
	}

	deleteIndex, err := service.Do(i.context)

	if err != nil {
		return err
	}

	if !deleteIndex.Acknowledged {
		return errors.New("The index deletion was not acknowledged.")
	}

	return nil
}

// ListIndices lists all open inidces on an elsasticsearch cluster
func (i *Indexer) ListIndices() ([]string, error) {
	return i.client.IndexNames()
}

// ListAllIndices lists all indices on and elasticsearch cluster
func (i *Indexer) ListAllIndices() ([]string, error) {
	catIndicesResponse, err := i.client.CatIndices().Columns("index").Do(i.context)

	if err != nil {
		return nil, err
	}

	container, err := toGabsContainer(catIndicesResponse)

	if err != nil {
		return nil, err
	}

	children, err := container.Children()

	if err != nil {
		return nil, err
	}

	indices := []string{}

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
func (i *Indexer) Settings(names ...string) (map[string]*gabs.Container, error) {
	indicesSettings, err := i.client.IndexGetSettings(names...).Do(i.context)

	if err != nil {
		return nil, err
	}

	settings := map[string]*gabs.Container{}

	for key, value := range indicesSettings {
		container, err := toGabsContainer(value.Settings)

		if err != nil {
			return nil, err
		}

		settings[key] = container
	}

	return settings, nil
}

// GetTask retrieves the status of a given task by task id
func (i *Indexer) GetTask(taskId string) (*gabs.Container, error) {
	return parse(i.client.TasksGetTask().TaskId(taskId).Do(i.context))
}

// GetClusterHealth returns the health status of the cluster
func (i *Indexer) GetClusterHealth(indices ...string) (*gabs.Container, error) {
	return parse(i.client.ClusterHealth().Index(indices...).Do(i.context))
}

// GetIndices returns index information for the provided indices
func (i *Indexer) GetIndices(indices ...string) (*gabs.Container, error) {
	return parse(i.client.IndexGet(indices...).Do(i.context))
}

// PutSettings updates elasticsearch indices settings
func (i *Indexer) PutSettings(body string, indices ...string) (*gabs.Container, error) {
	service := i.client.IndexPutSettings(indices...).BodyString(body)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
	}

	return parse(service.Do(i.context))
}

// FieldMappings returns the field mappings for the specified indices
func (i *Indexer) FieldMappings(indices ...string) (*gabs.Container, error) {
	service := i.client.GetFieldMapping().Index(indices...)

	if i.options != nil && i.options.IgnoreUnavailable {
		service.IgnoreUnavailable(i.options.IgnoreUnavailable)
	}

	return parse(service.Do(i.context))
}

// CreateRepository creates a snapshot repository
func (i *Indexer) CreateRepository(repository string, repoType string, verify bool, settings map[string]interface{}) (*gabs.Container, error) {
	service := i.client.SnapshotCreateRepository(repository).Type(repoType).Verify(verify).Settings(settings)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
	}

	return parse(service.Do(i.context))
}

// DeleteRepositories deletes one or many snapshot repositories
func (i *Indexer) DeleteRepositories(repositories ...string) (*gabs.Container, error) {
	service := i.client.SnapshotDeleteRepository(repositories...)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
	}

	return parse(service.Do(i.context))
}

// Snapshot takes a snapshot of one or more indices and stores it in the provided repository
func (i *Indexer) Snapshot(repository string, snapshot string, indices ...string) (*gabs.Container, error) {
	service := i.client.SnapshotCreate(repository, snapshot)

	if i.options != nil {
		service.WaitForCompletion(i.options.WaitForCompletion)
	}

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
	}

	body := map[string]interface{}{
		"ignore_unavailable":   i.options.IgnoreUnavailable,
		"include_global_state": i.options.IncludeGlobalState,
		"partial":              i.options.Partial,
		"indices":              strings.Join(indices, ","),
	}

	bodyJson, err := toGabsContainer(body)

	if err != nil {
		return nil, err
	}

	return parse(service.BodyString(bodyJson.String()).Do(context.Background()))
}

// GetSnapshots retrives information regarding snapshots in a given repository
func (i *Indexer) GetSnapshots(repository string, snapshot string) (*gabs.Container, error) {
	service := i.client.SnapshotGet(repository)

	if snapshot != "*" && len(snapshot) > 0 {
		service.Snapshot(snapshot)
	}

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
	}

	if i.options != nil && i.options.IgnoreUnavailable {
		service.IgnoreUnavailable(i.options.IgnoreUnavailable)
	}

	return parse(service.Do(i.context))
}

// ListSnapshots returns a list of snapshots for the given repository
func (i *Indexer) ListSnapshots(repository string) ([]string, error) {
	response, err := i.GetSnapshots(repository, "*")

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

// DeleteSnapshot deletes a snapshot for a given repository
func (i *Indexer) DeleteSnapshot(repository string, name string) (*gabs.Container, error) {
	return parse(i.client.SnapshotDelete(repository, name).Do(i.context))
}

// SnapshotRestore restores a snapshot from the specified repository
func (i *Indexer) SnapshotRestore(repository string, snapshot string) (*gabs.Container, error) {
	service := i.client.SnapshotRestore(repository, snapshot)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
	}

	if i.options != nil && len(i.options.Indices) > 0 {
		service.Indices(i.options.Indices...)
	}

	if i.options != nil && len(i.options.IndexSettings) > 0 {
		service.IndexSettings(i.options.IndexSettings)
	}

	if i.options != nil && len(i.options.RenamePattern) > 0 {
		service.RenamePattern(i.options.RenamePattern)
	}

	if i.options != nil && len(i.options.RenameReplacement) > 0 {
		service.RenameReplacement(i.options.RenameReplacement)
	}

	if i.options != nil {
		service.WaitForCompletion(i.options.WaitForCompletion).
			IncludeAliases(i.options.IncludeAliases).Partial(i.options.Partial).
			IncludeGlobalState(i.options.IncludeGlobalState)
	}

	return parse(service.Do(i.context))
}

// Recovery checks the indices recovery status when restoring a snapshot
func (i *Indexer) Recovery(indices ...string) (map[string]*gabs.Container, error) {
	response, err := i.client.IndicesRecovery().Human(true).Indices(indices...).Do(i.context)

	if err != nil {
		return nil, err
	}

	indicesRecoveryResponse := map[string]*gabs.Container{}

	for index, recovery := range response {
		container, err := toGabsContainer(recovery)

		if err != nil {
			return nil, err
		}

		indicesRecoveryResponse[index] = container
	}

	return indicesRecoveryResponse, nil
}

// Close closes an elasticsearch index
func (i *Indexer) Close(name string) (*gabs.Container, error) {
	service := i.client.CloseIndex(name)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
	}

	return parse(service.Do(i.context))
}

// Open opens an elasticsearch index
func (i *Indexer) Open(name string) (*gabs.Container, error) {
	service := i.client.OpenIndex(name)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
	}

	return parse(service.Do(i.context))
}

// IndexCat retrieves information associated to the given index
func (i *Indexer) IndexCat(name string) (*gabs.Container, error) {
	return parse(i.client.CatIndices().Index(name).Columns(columns...).Do(i.context))
}

// AliasesCat retrives information associated to all current index aliases
func (i *Indexer) AliasesCat() (*gabs.Container, error) {
	return parse(i.client.CatAliases().Columns("*").Do(i.context))
}

// AddAlias adds an alias to a given elasticsearch index
func (i *Indexer) AddAlias(indexName string, aliasName string) (*gabs.Container, error) {
	return parse(elastic.NewAliasService(i.client).Add(indexName, aliasName).Do(i.context))
}

// AddAliasByAction adds an alias by *elastic.AliasAddAction
func (i *Indexer) AddAliasByAction(aliasAction *elastic.AliasAddAction) (*gabs.Container, error) {
	return parse(elastic.NewAliasService(i.client).Action(aliasAction).Do(i.context))
}

// RemoveIndexFromAlias removes an index from a given alias
func (i *Indexer) RemoveIndexFromAlias(index string, alias string) (*gabs.Container, error) {
	return parse(elastic.NewAliasService(i.client).Remove(index, alias).Do(i.context))
}

// AliasAddAction returns an instances of *elastic.AliasAddAction
func (i *Indexer) AliasAddAction(alias string) *elastic.AliasAddAction {
	return elastic.NewAliasAddAction(alias)
}

// IndexStats retrieves the statistics for the given indices
func (i *Indexer) IndexStats(indices ...string) (map[string]*gabs.Container, error) {
	indexStatsResponse, err := i.client.IndexStats(indices...).Do(i.context)

	if err != nil {
		return nil, err
	}

	mapContainer := map[string]*gabs.Container{}

	for index, indexStats := range indexStatsResponse.Indices {
		container, err := toGabsContainer(indexStats)

		if err != nil {
			return nil, err
		}

		mapContainer[index] = container
	}

	return mapContainer, nil
}

// Rollover executes an index rollover if the given conditions are met
func (i *Indexer) Rollover(alias, newIndex, maxAge, maxSize string, maxDocs int64, settings map[string]interface{}) (*gabs.Container, error) {
	service := i.client.RolloverIndex(alias)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
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

	return parse(service.Do(i.context))
}
