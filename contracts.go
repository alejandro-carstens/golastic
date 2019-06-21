package golastic

import (
	"github.com/Jeffail/gabs"
	elastic "github.com/alejandro-carstens/elasticfork"
)

type ElasticModelable interface {
	GetIndexMappings() map[string]interface{}

	Properties() []string

	Index() string

	GetId() string

	Validate() error

	PropertiesMap() map[string]interface{}

	Data() map[string]interface{}

	IsGolasticModel() bool
}

type QueryBuildable interface {
	SetModel(model ElasticModelable) (QueryBuildable, error)

	Where(field string, operand string, value interface{}) QueryBuildable

	WhereIn(field string, values []interface{}) QueryBuildable

	WhereNotIn(field string, values []interface{}) QueryBuildable

	Filter(field string, operand string, value interface{}) QueryBuildable

	FilterIn(field string, values []interface{}) QueryBuildable

	Match(field string, operand string, value interface{}) QueryBuildable

	MatchIn(field string, values []interface{}) QueryBuildable

	MatchNotIn(field string, values []interface{}) QueryBuildable

	OrderBy(field string, order bool) QueryBuildable

	Limit(value int) QueryBuildable

	From(value int) QueryBuildable

	GroupBy(fields ...string) QueryBuildable
}

type Indexable interface {
	CreateIndex(name string, schema string) error

	DeleteIndex(name string) error

	Exists(name string) (bool, error)

	ListIndices() ([]string, error)

	ListAllIndices() ([]string, error)

	AliasesCat() ([]*CatAliasesResponse, error)

	Settings(names ...string) (map[string]*gabs.Container, error)

	GetClusterHealth(indices ...string) (*gabs.Container, error)

	GetIndices(indices ...string) (*gabs.Container, error)

	AddAlias(indexName string, aliasName string) (*gabs.Container, error)
}

type Clientable interface {
	SetClient(*elastic.Client)
}

type Executable interface {
	Insert(models ...ElasticModelable) (*WriteResponse, error)

	Delete(ids ...string) (*WriteResponse, error)

	Update(models ...ElasticModelable) (*WriteResponse, error)

	Aggregate() (map[string]*AggregationResponse, error)

	Get(models interface{}) error

	Execute(params map[string]interface{}) (*WriteByQueryResponse, error)

	Destroy() (*WriteByQueryResponse, error)

	Count() (int64, error)

	Cursor(offset int, sortValues []interface{}, models interface{}) ([]interface{}, error)

	MinMax(field string, isDateField bool) (*MinMaxResponse, error)
}

type Queryable interface {
	Indexable

	QueryBuildable

	Clientable

	Executable
}
