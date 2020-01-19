package examples

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/alejandro-carstens/golastic"
	"github.com/bxcodec/faker"
	"github.com/rs/xid"
)

func Connect() (*golastic.Builder, error) {
	connection := golastic.NewConnection(&golastic.ConnectionContext{
		Urls:                []string{os.Getenv("ELASTICSEARCH_URI")},
		Password:            os.Getenv("ELASTICSEARCH_PASSWORD"),
		Username:            os.Getenv("ELASTICSEARCH_USERNAME"),
		HealthCheckInterval: 30,
		Context:             context.Background(),
	})

	if err := connection.Connect(); err != nil {
		return nil, err
	}

	if err := createIndex(connection); err != nil {
		return nil, err
	}

	time.Sleep(1 * time.Second)

	builder := connection.Builder("movies")

	if err := seedMovies(builder); err != nil {
		log.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	return builder, nil
}

func createIndex(connection *golastic.Connection) error {
	schema := map[string]interface{}{
		"settings": map[string]int{
			"number_of_shards":   1,
			"number_of_replicas": 1,
		},
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"id": map[string]interface{}{
					"type":  "keyword",
					"index": true,
				},
				"title": map[string]interface{}{
					"type":  "keyword",
					"index": true,
				},
				"description": map[string]interface{}{
					"type": "text",
				},
				"rating": map[string]interface{}{
					"type":  "keyword",
					"index": true,
				},
				"cast": map[string]interface{}{
					"type": "nested",
					"properties": map[string]interface{}{
						"director": map[string]interface{}{
							"type":  "keyword",
							"index": true,
						},
						"total": map[string]interface{}{
							"type":  "integer",
							"index": true,
						},
					},
				},
				"release_date": map[string]interface{}{
					"type": "date",
				},
			},
		},
	}

	b, err := json.Marshal(schema)

	if err != nil {
		return err
	}

	return connection.Indexer(nil).CreateIndex("movies", string(b))
}

func seedMovies(builder *golastic.Builder) error {
	count := 0

	movies := []interface{}{}

	for {
		m, err := makeMovie()

		if err != nil {
			return err
		}

		movies = append(movies, m)

		count++

		if count == 1000 {
			break
		}
	}

	_, err := builder.Insert(movies...)

	return err
}

type Movie struct {
	Id          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Rating      string `json:"rating"`
	Views       int    `json:"views"`
	Cast        struct {
		Director string `json:"director"`
		Total    int    `json:"total"`
	} `json:"cast"`
	ReleaseDate time.Time `json:"release_date"`
}

var ratings []string = []string{"R", "PG", "F"}
var views []int = []int{500000, 600000, 700000}

func makeMovie() (Movie, error) {
	m := Movie{}

	if err := faker.FakeData(&m); err != nil {
		return m, err
	}

	m.Id = xid.New().String()
	m.ReleaseDate = time.Now().AddDate(-rand.Intn(20), -rand.Intn(11), rand.Intn(28))

	index := int(math.Abs(float64(rand.Intn(3) - 1)))

	if index == 0 {
		m.Cast.Director = "James Cameron"
		m.Cast.Total = 100
	}

	m.Rating = ratings[index]
	m.Views = views[index]

	return m, nil
}
