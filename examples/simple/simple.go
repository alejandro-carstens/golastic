package main

import (
	"encoding/json"
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

	log.Println(string(b))
}
