package main

import (
	"log"

	"github.com/alejandro-carstens/golastic/examples"
)

func main() {
	builder, err := examples.Connect()

	if err != nil {
		log.Fatal(err)
	}

	builder.WhereIn("rating", []interface{}{"R"}).WhereNested("cast.director", "=", "James Cameron")

	count, err := builder.Count()

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Pre-update count: %v", count)

	params := map[string]interface{}{
		"rating":    "PG",
		"new_field": "whatever",
	}

	response, err := builder.Execute(params)

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Response: %v", response.String())

	count, err = builder.Count()

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Post-update count: %v", count)

	builder.Clear().Where("rating", "=", "PG").Filter("new_field", "=", "whatever")

	count, err = builder.Count()

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Updated count: %v", count)
}
