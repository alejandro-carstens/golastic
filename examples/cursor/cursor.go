package main

import (
	"encoding/json"
	"log"

	"github.com/alejandro-carstens/golastic/examples"
)

func main() {
	var err error

	builder, err := examples.Connect()

	if err != nil {
		log.Fatal(err)
	}

	builder.
		WhereIn("rating", []interface{}{"R"}).
		WhereNested("cast.director", "=", "James Cameron").
		OrderBy("release_date", true)

	var cursor []interface{}

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
			log.Fatal(err)
		}

		log.Printf("Response: %v", string(b))
	}
}
