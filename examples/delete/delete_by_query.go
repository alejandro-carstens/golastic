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

	log.Printf("Pre-delete count: %v", count)

	response, err := builder.Destroy()

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Response: %v", response.String())

	builder.WhereIn("rating", []interface{}{"R"}).WhereNested("cast.director", "=", "James Cameron")

	count, err = builder.Count()

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Post-delete count: %v", count)
}
