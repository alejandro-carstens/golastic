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
