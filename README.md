[![Build Status](https://travis-ci.org/alejandro-carstens/golastic.svg?branch=master)](https://travis-ci.org/alejandro-carstens/golastic) [![Go Report Card](https://goreportcard.com/badge/github.com/alejandro-carstens/golastic)](https://goreportcard.com/report/github.com/alejandro-carstens/golastic) [![GoDoc](https://godoc.org/github.com/alejandro-carstens/golastic?status.svg)](https://godoc.org/github.com/alejandro-carstens/golastic) [![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/alejandro-carstens/golastic/blob/master/LICENSE)

<p align="center">
  <img src="https://github.com/alejandro-carstens/golastic/blob/master/logo.png">
</p>

Golastic is meant to be a simple and intuitive programmatic query DSL implementation for Elasticsearch. It intends to provide a convenient and fluent interface for creating and running Elasticsearch queries as well as for performing different indices operations.

## Getting Started

To start using this package in your application simply run: ```go get github.com/alejandro-carstens/golastic```

## Usage

Establish a connection:
```go

import (
    "os"
    
    "github.com/alejandro-carstens/golastic"
)

func main() {
	connection := golastic.NewConnection(
		&golastic.ConnectionContext{
			Urls:                []string{os.Getenv("ELASTICSEARCH_URI")},
			Password:            os.Getenv("ELASTICSEARCH_PASSWORD"),
			Username:            os.Getenv("ELASTICSEARCH_USERNAME"),
			HealthCheckInterval: 30,
		},
	)

        if err := connection.Connect(); err != nil {
                // Handle error
        }
  
        // Do something else here
}

```

Create a builder:
```go
	doc := &Example{
		Id:          "awesome_unique_id",
		Description: "This is an awesome description",
	}
	
	builder := connection.Builder("your_index")
	
	response, err := builder.Insert(doc)
	
	// Handle response and error
```

Create an indexer:
```go
	config := map[string]interface{}{
		"settings": // settings...,
		"mappings": // mappings...,
	}
	
	schema, err := json.Marshal(config)
	
	if err != nil {
		// Handle error
	}
	
	options := &golastic.IndexOptions{
		WaitForCompletion: true,
		IgnoreUnavailable: true,
		// More options...
	}
	
	indexer := connection.Indexer(options)
	
	if err := indexer.CreateIndex("your_index", string(schema); err != nil {
		// Handle error
	}
```
### Building Queries

Golastic provides the following clauses for building queries:
