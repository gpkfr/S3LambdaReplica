package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mitchellh/mapstructure"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"
)

type Config struct {
	Region       string   `json:region`
	Destinations []string `json:destinations`
}

var config map[string]Config

func parseConfig() (err error) {
	//First we make the hash (map)
	config = make(map[string]Config)
	data := make([]byte, 0)

	configURL := os.Getenv("CONFIG_URL")
	if configURL != "" {
		u, err := url.Parse(configURL)
		if err != nil {
			return err
		}

		if u.IsAbs() == false {
			u.Scheme = "https"
		}

		//is url need transport
		switch u.Scheme {
		case "https":
			tr := &http.Transport{
				MaxIdleConns:       10,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: true,
			}
			client := &http.Client{Transport: tr}
			resp, err := client.Get(u.String())
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			data, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				return err
			}

		case "http":
			fmt.Println("HTTP")

		default:
			fmt.Println("Unsupported config url Scheme")
		}
	} else {
		//we use CONFIG ENV
		data, err = base64.StdEncoding.DecodeString(os.Getenv("CONFIG"))
		if err != nil {
			return fmt.Errorf(" Base64 decode Error  : %v", err)
		}
	}
	// now we test if config is valid
	if len(data) == 0 {
		return fmt.Errorf("Unable to get configuration")
	}

	//then we unmarshal data to config hash table
	json.Unmarshal(data, &config)
	if err != nil {
		return fmt.Errorf("Error unmarshal %v", err)
	}
	//todo :
	// avoid Cyclic ref
	// S3 A -> B -> A
	log.Println("Parseconfig ok")
	return nil
}

func HandleEvent(Ctx context.Context, event interface{}) error {
	err := parseConfig()
	if err != nil {
		log.Fatal("ParseConfig Error")
	}

	e, s3Event := event.(map[string]interface{}), events.S3Event{}

	if mapstructure.Decode(e, &s3Event); len(s3Event.Records) > 0 && s3Event.Records[0].S3.Object.Key != "" {
		log.Println("S3 events received")
		return processS3Event(s3Event)
	}
	return nil
}

func processS3Event(s3evt events.S3Event) (err error) {
	//make a channel for err
	errChan := make(chan error)

	// read events
	for _, v := range s3evt.Records {
		log.Println("Moving", v.S3.Bucket.Name, v.S3.Object.Key, "To", config[v.S3.Bucket.Name].Destinations)
		//open an aws Session
		sess, err := session.NewSession(&aws.Config{Region: aws.String(config[v.S3.Bucket.Name].Region)})
		if err != nil {
			return fmt.Errorf("unable to enstablish aws session for %v", config[v.S3.Bucket.Name])
		}
		// go into Destinations
		for _, v1 := range config[v.S3.Bucket.Name].Destinations {
			go copyObject(s3.New(sess), v.S3.Bucket.Name, v1, v.S3.Object.Key, errChan)
		}
	}

	for _, v := range s3evt.Records {
		for range config[v.S3.Bucket.Name].Destinations {
			err = <-errChan
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func copyObject(svc *s3.S3, from, to, item string, errChan chan error) {
	_, err := svc.CopyObject(&s3.CopyObjectInput{Bucket: aws.String(to), CopySource: aws.String(from + "/" + item), Key: aws.String(item)})
	if err != nil {
		errChan <- fmt.Errorf("Unable to copy %s from bucket %q to bucket %q, %v", item, from, to, err)
		return
	}

	err = svc.WaitUntilObjectExists(&s3.HeadObjectInput{Bucket: aws.String(to), Key: aws.String(item)})
	if err != nil {
		errChan <- fmt.Errorf("error occured while waiting for item %q to be copied in bucket %q, %v", item, to, err)
		return
	}
}

/*func init() {
	_ = os.Setenv("CONFIG", "ewogICJndGNwc3JjIjogewogICAgInJlZ2lvbiI6ICJ1cy1lYXN0LTEiLAogICAgImRlc3RpbmF0aW9ucyI6IFsKICAgICAgImd0Y3BkZXN0IgogIF0KICB9Cn0=")
}*/

func main() {
	lambda.Start(HandleEvent)
}
