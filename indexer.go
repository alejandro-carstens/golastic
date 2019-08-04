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

type indexer struct {
	options *IndexOptions
	client  *elastic.Client
}

// SetOptions sets the index options for the action to be performed
func (i *indexer) SetOptions(options *IndexOptions) {
	i.options = options
}

// Exists checks if a given index exists on ElasticSearch
func (i *indexer) Exists(name string) (bool, error) {
	return i.client.IndexExists(name).Do(context.Background())
}

// CreateIndex creates and ElasticSearch index
func (i *indexer) CreateIndex(name string, schema string) error {
	service := i.client.CreateIndex(name)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.Timeout(i.options.Timeout)
	}

	createIndex, err := service.BodyString(schema).Do(context.Background())

	if err != nil {
		return err
	}

	if !createIndex.Acknowledged {
		return errors.New("The index was not acknowledged.")
	}

	return nil
}

// DeleteIndex deletes an ElasticSearch Index
func (i *indexer) DeleteIndex(name string) error {
	service := i.client.DeleteIndex(name)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.Timeout(i.options.Timeout)
	}

	deleteIndex, err := service.Do(context.Background())

	if err != nil {
		return err
	}

	if !deleteIndex.Acknowledged {
		return errors.New("The index deletion was not acknowledged.")
	}

	return nil
}

// ListIndices lists all open inidces on an elsasticsearch cluster
func (i *indexer) ListIndices() ([]string, error) {
	return i.client.IndexNames()
}

// ListAllIndices lists all indices on and elasticsearch cluster
func (i *indexer) ListAllIndices() ([]string, error) {
	catIndicesResponse, err := i.client.CatIndices().Columns("index").Do(context.Background())

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
func (i *indexer) Settings(names ...string) (map[string]*gabs.Container, error) {
	indicesSettings, err := i.client.IndexGetSettings(names...).Do(context.Background())

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
func (i *indexer) GetTask(taskId string) (*gabs.Container, error) {
	return parse(i.client.TasksGetTask().TaskId(taskId).Do(context.Background()))
}

// GetClusterHealth returns the health status of the cluster
func (i *indexer) GetClusterHealth(indices ...string) (*gabs.Container, error) {
	return parse(i.client.ClusterHealth().Index(indices...).Do(context.Background()))
}

// GetIndices returns index information for the provided indices
func (i *indexer) GetIndices(indices ...string) (*gabs.Container, error) {
	return parse(i.client.IndexGet(indices...).Do(context.Background()))
}

func (i *indexer) PutSettings(body string, indices ...string) (*gabs.Container, error) {
	service := i.client.IndexPutSettings(indices...).BodyString(body)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
	}

	return parse(service.Do(context.Background()))
}

// CreateRepository creates a snapshot repository
func (i *indexer) CreateRepository(repository string, repoType string, verify bool, settings map[string]interface{}) (*gabs.Container, error) {
	service := i.client.SnapshotCreateRepository(repository).Type(repoType).Verify(verify).Settings(settings)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
	}

	return parse(service.Do(context.Background()))
}

// DeleteRepositories deletes one or many snapshot repositories
func (i *indexer) DeleteRepositories(respositories ...string) (*gabs.Container, error) {
	service := i.client.SnapshotDeleteRepository(respositories...)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
	}

	return parse(service.Do(context.Background()))
}

// Snaphsot takes a snapshot of one or more indices and stores it in the provided repository
func (i *indexer) Snapshot(repository string, snapshot string, indices ...string) (*gabs.Container, error) {
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
func (i *indexer) GetSnapshots(respository string, snapshot string) (*gabs.Container, error) {
	service := i.client.SnapshotGet(respository)

	if snapshot != "*" && len(snapshot) > 0 {
		service.Snapshot(snapshot)
	}

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
	}

	if i.options != nil && i.options.IgnoreUnavailable {
		service.IgnoreUnavailable(i.options.IgnoreUnavailable)
	}

	return parse(service.Do(context.Background()))
}

// ListSnapshots returns a list of snapshots for the given repository
func (i *indexer) ListSnapshots(repository string) ([]string, error) {
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
func (i *indexer) DeleteSnapshot(repository string, name string) (*gabs.Container, error) {
	return parse(i.client.SnapshotDelete(repository, name).Do(context.Background()))
}

// SnapshotRestore restores a snapshot from the specified repository
func (i *indexer) SnapshotRestore(repository string, snapshot string) (*gabs.Container, error) {
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

	return parse(service.Do(context.Background()))
}

// Recovery checks the indices recovery status when restoring a snapshot
func (i *indexer) Recovery(indices ...string) (map[string]*gabs.Container, error) {
	response, err := i.client.IndicesRecovery().Human(true).Indices(indices...).Do(context.Background())

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
func (i *indexer) Close(name string) (*gabs.Container, error) {
	service := i.client.CloseIndex(name)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
	}

	return parse(service.Do(context.Background()))
}

// Close closes an elasticsearch index
func (i *indexer) Open(name string) (*gabs.Container, error) {
	service := i.client.OpenIndex(name)

	if i.options != nil && len(i.options.Timeout) > 0 {
		service.MasterTimeout(i.options.Timeout)
	}

	return parse(service.Do(context.Background()))
}

// IndexCat retrieves information assocaited to the given index
func (i *indexer) IndexCat(name string) (*gabs.Container, error) {
	return parse(i.client.CatIndices().Index(name).Columns(columns...).Do(context.Background()))
}

// AliasesCat retrives information assocaited to all current index aliases
func (i *indexer) AliasesCat() (*gabs.Container, error) {
	return parse(i.client.CatAliases().Columns("*").Do(context.Background()))
}

// AddAlias adds an alias to a given elasticsearch index
func (i *indexer) AddAlias(indexName string, aliasName string) (*gabs.Container, error) {
	return parse(elastic.NewAliasService(i.client).Add(indexName, aliasName).Do(context.Background()))
}

// AddAliasByAction adds an alias by *elastic.AliasAddAction
func (i *indexer) AddAliasByAction(aliasAction *elastic.AliasAddAction) (*gabs.Container, error) {
	return parse(elastic.NewAliasService(i.client).Action(aliasAction).Do(context.Background()))
}

// RemoveIndexFromAlias removes an index from a given alias
func (i *indexer) RemoveIndexFromAlias(index string, alias string) (*gabs.Container, error) {
	return parse(elastic.NewAliasService(i.client).Remove(index, alias).Do(context.Background()))
}

// AliasAddAction returns an instances of *elastic.AliasAddAction
func (i *indexer) AliasAddAction(alias string) *elastic.AliasAddAction {
	return elastic.NewAliasAddAction(alias)
}

// IndexStats retrieves the statistics for the given indices
func (i *indexer) IndexStats(indices ...string) (map[string]*gabs.Container, error) {
	indexStatsResponse, err := i.client.IndexStats(indices...).Do(context.Background())

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
func (i *indexer) Rollover(alias, newIndex, maxAge, maxSize string, maxDocs int64, settings map[string]interface{}) (*gabs.Container, error) {
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

	return parse(service.Do(context.Background()))
}
