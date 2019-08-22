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
		OrderBy("release_date", true)

	var cursor []interface{} = nil
	var err error = nil

	for {
		movies := []examples.Movie{}

		cursor, err = builder.Cursor(100, cursor, &movies)

		if err != nil {
			log.Fatal(err)
		}

		if len(movies) < 100 {
			log.Println("Done.")

			return
		}

		b, err := json.Marshal(map[string]interface{}{
			"movies": movies,
			"count":  len(movies),
			"cursor": cursor,
		})

		if err != nil {
			log.Println(err)
		}

		log.Println(string(b))
	}
}
