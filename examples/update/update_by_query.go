package main

import (
	"log"
	"os"
	"time"

	"github.com/alejandro-carstens/golastic"
	"github.com/alejandro-carstens/golastic/examples"
)

func main() {
	connection := golastic.NewConnection(&golastic.ConnectionContext{
		Urls:                []string{os.Getenv("ELASTICSEARCH_URI")},
		Password:            os.Getenv("ELASTICSEARCH_PASSWORD"),
		Username:            os.Getenv("ELASTICSEARCH_USERNAME"),
		HealthCheckInterval: 30,
	})

	if err := connection.Connect(); err != nil {
		log.Fatal(err)
	}

	if err := examples.CreateIndex(connection); err != nil {
		log.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	builder := connection.Builder("movies")

	if err := examples.SeedMovies(builder); err != nil {
		log.Fatal(err)
	}

	time.Sleep(1 * time.Second)

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
