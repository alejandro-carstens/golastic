package main

import (
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

	if err := createIndex(connection); err != nil {
		log.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	builder := connection.Builder("movies")

	if err := seedMovies(builder); err != nil {
		log.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	builder.
		WhereIn("rating", []interface{}{"R"}).
		WhereNested("cast.director", "=", "James Cameron").
		OrderBy("release_date", true).
		Limit(15)

	movies := []movie{}

	if err := builder.Get(&movies); err != nil {
		log.Fatal(err)
	}

	builder.GroupBy("rating").Limit(100)

	response, err := builder.Aggregate()

	if err != nil {
		log.Fatal(err)
	}

	b, err := json.Marshal(map[string]interface{}{
		"movies":   movies,
		"response": response,
	})

	if err != nil {
		log.Fatal(err)
	}

	log.Println(string(b))
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

type movie struct {
	Id          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Rating      string `json:"rating"`
	Cast        struct {
		Director string `json:"director"`
		Total    int    `json:"total"`
	} `json:"cast"`
	ReleaseDate time.Time `json:"release_date"`
}

var ratings []string = []string{"R", "PG", "F"}

func makeMovie() (movie, error) {
	m := movie{}

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

	return m, nil
}
