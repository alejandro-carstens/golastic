package golastic

import (
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	elastic "github.com/alejandro-carstens/elasticfork"
	"github.com/sha1sum/aws_signing_client"
)

type ElasticsearchClient struct {
	client *elastic.Client
}

// SetClient sets an Elastic Client
func (ec *ElasticsearchClient) InitClient() error {
	switch os.Getenv("ELASTICSEARCH_DRIVER") {
	case "aws":
		client, err := ec.getAwsClient()

		if err != nil {
			return err
		}

		ec.client = client
		break
	default:
		client, err := elastic.NewClient(
			elastic.SetURL(os.Getenv("ELASTICSEARCH_URI")),
			elastic.SetSniff(true),
			elastic.SetHealthcheckInterval(30*time.Second),
			elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),
			elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)),
			elastic.SetBasicAuth(os.Getenv("ELASTIC_USERNAME"), os.Getenv("ELASTIC_PASSWORD")),
		)

		if err != nil {
			return err
		}

		ec.client = client
	}

	return nil
}

func (ec *ElasticsearchClient) SetClient(client *elastic.Client) {
	ec.client = client
}

func (ec *ElasticsearchClient) GetClient() *elastic.Client {
	return ec.client
}

func (ec *ElasticsearchClient) getAwsClient() (*elastic.Client, error) {
	creds := credentials.NewStaticCredentials(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")

	signer := v4.NewSigner(creds)

	client, err := aws_signing_client.New(signer, nil, "es", os.Getenv("AWS_ELASTICSEARCH_REGION"))

	if err != nil {
		return nil, err
	}

	return elastic.NewClient(
		elastic.SetURL(os.Getenv("AWS_ELASTICSEARCH_URL")),
		elastic.SetScheme("https"),
		elastic.SetHttpClient(client),
		elastic.SetSniff(false),
	)
}
