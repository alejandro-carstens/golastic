[![Build Status](https://travis-ci.org/alejandro-carstens/golastic.svg?branch=master)](https://travis-ci.org/alejandro-carstens/golastic) [![Go Report Card](https://goreportcard.com/badge/github.com/alejandro-carstens/golastic)](https://goreportcard.com/report/github.com/alejandro-carstens/golastic) [![GoDoc](https://godoc.org/github.com/alejandro-carstens/golastic?status.svg)](https://godoc.org/github.com/alejandro-carstens/golastic) [![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/alejandro-carstens/golastic/blob/master/LICENSE)

<p align="center">
  <img src="https://github.com/alejandro-carstens/golastic/blob/master/logo.png">
</p>

Golastic is meant to be a simple and intuitive programmatic query builder implementation for Elasticsearch. It intends to provide a convenient and fluent interface for creating and running Elasticsearch queries as well as for performing different indices operations.

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

### Using the Indexer

Please checkout the godoc [Indexer](https://godoc.org/github.com/alejandro-carstens/golastic#Indexer) section for more detailed documentation on the Indexer API.

### Building Queries

Golastic provides the following clauses for building queries:

#### Where Clauses

Where clauses map to ```must``` + ```term``` queries in Elasticsearch, meaning that there will be a look up for the exact search term on an inverted index

* Where (```=, <>, >, <, <=, >=```)
* WhereIn
* WhereNotIn

Ex:

```go
	players := []interface{}{"player1", "player2", "palyer3"}
	games := []interface{}{"game4", "game5"}
	
	builder := connection.Builder("your_index")
	
	builder.Where("level", ">", 2).WhereIn("player", players).WhereNotIn("game", games)
	
	response := []Response{} // It can also be map[string]interface{}{}
	
	if err := builder.Get(&response); err != nil {
		// Handle error
	}
```

#### Match Clauses
Match clauses map to ```must``` + ```match``` queries in Elasticsearch, which means that an analyzer will be applied to the search term and will therefore try to match what is stored on a given index

* Match (```=, <>```)
* MatchIn
* MatchNotIn

Ex:

```go
	players := []interface{}{"player1", "player2", "palyer3"}
	games := []interface{}{"game4", "game5"}
	
	builder := connection.Builder("your_index")
	
	builder.Match("level", "<>", 7).MatchIn("player", players).MatchNotIn("game", games)
	
	response := []Response{} // It can also be map[string]interface{}{}
	
	if err := builder.Get(&response); err != nil {
		// Handle error
	}
```

#### Filter Clauses
Filter clauses map to ```filter``` + ```term``` queries in Elasticsearch. Filter queries do not use the ```_score``` field for returned results, they just return the results that match the query criteria

* Filter (```=, <>, >, <, <=, >=```)
* FilterIn

Ex:

```go
	players := []interface{}{"player1", "player2", "palyer3"}
	
	builder := connection.Builder("your_index")
	
	builder.Filter("level", ">=", 7).FilterIn("player", players)
	
	response := []Response{} // It can also be map[string]interface{}{}
	
	if err := builder.Get(&response); err != nil {
		// Handle error
	}
```

#### Nested Clauses
Golastic provides the ability to perform nested queries using all the previous clauses ```WhereNested, WhereInNested, WhereNotInNested, FilterNested, FilterInNested, MatchNested, MatchInNested & MatchNotInNested```. Nested clauses are subjected to the same rules as their non-nested counter parts. However, it is important to specify the nested path using dot notation such as ```attribute.value```.

Ex:
```go
	players := []interface{}{"player1", "player2", "palyer3"}
	
	builder := connection.Builder("your_index")
	
	builder.FilterNested("video_game.level", ">=", 7).WhereNotInNested("video_game.player", players)
	
	response := []Response{} // It can also be map[string]interface{}{}
	
	if err := builder.Get(&response); err != nil {
		// Handle error
	}
```

#### Limit Clause
Limit clauses set the limit for the maximum number of documents to be returned

Ex:

```go
	players := []interface{}{"player1", "player2", "palyer3"}
	
	builder := connection.Builder("your_index")
	
	builder.Filter("level", ">=", 7).WhereIn("player", players).Limit(10)
	
	response := []Response{} // It can also be map[string]interface{}{}
	
	if err := builder.Get(&response); err != nil {
		// Handle error
	}
```

#### OrderBy Clause
OrderBy clauses set the sorting order in which the documents need to be returned. Use `true` for ascending and `false` for descending

Ex:

```go
	players := []interface{}{"player1", "player2", "palyer3"}
	
	builder := connection.Builder("your_index")
	
	builder.Filter("level", ">=", 7).OrderBy("player", true).Limit(10)
	
	response := []Response{} // It can also be map[string]interface{}{}
	
	if err := builder.Get(&response); err != nil {
		// Handle error
	}
```

#### From Clause
From clauses set the offset from which the query will return documents

Ex:

```go
	players := []interface{}{"player1", "player2", "palyer3"}
	
	builder := connection.Builder("your_index")
	
	builder.Filter("level", ">=", 7).OrderBy("player", true).From(5).Limit(5)
	
	response := []Response{} // It can also be map[string]interface{}{}
	
	if err := builder.Get(&response); err != nil {
		// Handle error
	}
```
