package golastic

import (
	"log"
	"os"
	"time"

	elastic "github.com/alejandro-carstens/elasticfork"
)

type ConnectionContext struct {
	Urls                []string
	Sniff               bool
	HealthCheckInterval int64
	ErrorLogPrefix      string
	InfoLogPrefix       string
	Password            string
	Username            string
}

type connection struct {
	client  *elastic.Client
	context *ConnectionContext
}

// Init initializes an Elastic Client
func (c *connection) Connect() error {
	client, err := elastic.NewClient(
		elastic.SetURL(c.context.Urls...),
		elastic.SetSniff(c.context.Sniff),
		elastic.SetHealthcheckInterval(time.Duration(c.context.HealthCheckInterval)*time.Second),
		elastic.SetErrorLog(log.New(os.Stderr, c.context.ErrorLogPrefix, log.LstdFlags)),
		elastic.SetInfoLog(log.New(os.Stdout, c.context.InfoLogPrefix, log.LstdFlags)),
		elastic.SetBasicAuth(c.context.Username, c.context.Password),
	)

	if err != nil {
		return err
	}

	c.client = client

	return nil
}

func (c *connection) Indexer(options *IndexOptions) *indexer {
	return &indexer{
		client:  c.client,
		options: options,
	}
}

func (c *connection) Builder(index string) *ElasticsearchBuilder {
	return &ElasticsearchBuilder{
		client: c.client,
		index:  index,
	}
}