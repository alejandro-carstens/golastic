package main

import (
	"encoding/json"
	"golastic/examples"
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
		OrderBy("release_date", true)

	var sortValues []interface{} = nil

	for {
		movies := []examples.Movie{}

		sortValues, err := builder.Cursor(100, sortValues, &movies)

		if err != nil {
			log.Fatal(err)
		}

		if len(sortValues) == 0 {
			log.Println("Done.")
			return
		}

		b, err := json.Marshal(map[string]interface{}{
			"sortValues": sortValues,
		})

		if err != nil {
			log.Fatal(err)
		}

		log.Println(string(b))
	}
}
