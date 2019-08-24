package main

import (
	"encoding/json"
	"log"

	"github.com/alejandro-carstens/golastic/examples"
)

func main() {
	builder, err := examples.Connect()

	if err != nil {
		log.Fatal(err)
	}

	builder.
		WhereIn("rating", []interface{}{"R"}).
		WhereNested("cast.director", "=", "James Cameron").
		OrderBy("release_date", true).
		Limit(15)

	movies := []examples.Movie{}

	if err := builder.Get(&movies); err != nil {
		log.Fatal(err)
	}

	builder.GroupBy("rating", "views").Limit(10000)

	response, err := builder.Aggregate()

	if err != nil {
		log.Fatal(err)
	}

	b, err := json.Marshal(map[string]interface{}{
		"movies":       movies,
		"aggregations": response,
	})

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Response: %v", string(b))
}
