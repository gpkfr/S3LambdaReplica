package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/mitchellh/mapstructure"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	s "strings"
	"time"
)

type Config struct {
	Region       string   `json:"region"`
	Destinations []string `json:"destinations"`
	ACL string `json:"acl,omitempty"`
}

var config map[string]Config

//parseConfig : Get and parse the config from URL (Set by CONFIG_URL ENV) or Decode the Base64's value of
// CONFIG ENV.
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
			resp, err := http.Get(u.String())
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			data, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				return err
			}
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
		return fmt.Errorf("unable to get configuration")
	}

	//then we unmarshal data to config hash table
	err = json.Unmarshal(data, &config)
	if err != nil {
		return fmt.Errorf("error unmarshal %v", err)
	}
	//todo :
	// avoid Cyclic ref
	// S3 A -> B -> A
	log.Println("Parse config ok")
	return nil
}

func HandleEvent(Ctx context.Context, event interface{}) error {
	err := parseConfig()
	if err != nil {
		log.Fatal("ParseConfig Error")
	}

	e, s3Event := event.(map[string]interface{}), events.S3Event{}

	if _ = mapstructure.Decode(e, &s3Event); len(s3Event.Records) > 0 && s3Event.Records[0].S3.Object.Key != "" {
		return processS3Event(s3Event)
	}
	log.Println("Completed...")
	return nil
}

func processS3Event(s3evt events.S3Event) (err error) { //make a channel for err

	var sAction string
	var objectACL string

	eventName := "s3:" + s3evt.Records[0].EventName
	log.Printf("S3evt : %q", s3evt.Records)
	//log the kind of event
	log.Printf("S3 event : %s", eventName)

	errChan := make(chan error)

	switch eventName {
	case s3.EventS3ObjectCreatedPut, s3.EventS3ObjectCreatedCopy:
		sAction = "Copying"
		for _, v := range s3evt.Records {
			if config[v.S3.Bucket.Name].ACL != "" {
				objectACL = config[v.S3.Bucket.Name].ACL
			} else {
				objectACL = "private"
			}
			// go into Destinations
			for _, v1 := range config[v.S3.Bucket.Name].Destinations {
				targetRegion, bucketDestination := getTargetRegion(v1, config[v.S3.Bucket.Name].Region)
				log.Println(sAction, v.S3.Bucket.Name, v.S3.Object.Key, "To", bucketDestination[0], "In", targetRegion)
				sess, err := session.NewSession(&aws.Config{Region: aws.String(targetRegion)})
				if err != nil {
					return fmt.Errorf("unable to enstablish aws session for %v", config[v.S3.Bucket.Name])
				}
				go copyObject(s3.New(sess), v.S3.Bucket.Name, bucketDestination[0], v.S3.Object.Key, objectACL, errChan)

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

	case s3.EventS3ObjectRemovedDeleteMarkerCreated:
		sAction = "Deleting"
		for _, v := range s3evt.Records {
			for _, v1 := range config[v.S3.Bucket.Name].Destinations {
				targetRegion, bucketDestination := getTargetRegion(v1, config[v.S3.Bucket.Name].Region)
				log.Println(sAction, v.S3.Bucket.Name, v.S3.Object.Key, "from", bucketDestination[0])
				sess, err := session.NewSession(&aws.Config{Region: aws.String(targetRegion)})
				if err != nil {
					return fmt.Errorf("unable to enstablish aws session for %v", config[v.S3.Bucket.Name])
				}
				go removeObject(s3.New(sess), bucketDestination[0], v.S3.Object.Key, errChan)

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

	default:
		sAction = eventName
	}
	return nil
}

func getTargetRegion(targetBucket, defaultRegion string) (targetRegion string, bucketDestination []string) {
	//Todo: Be more determinist with region
	bucketDestination = s.Split(targetBucket, "@")
	if len(bucketDestination) > 1 {
		targetRegion = bucketDestination[1]
	} else {
		sess := session.Must(session.NewSession())
		S3Region, err := s3manager.GetBucketRegion(aws.BackgroundContext(), sess, bucketDestination[0], "us-east-1")
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
				log.Printf("Unable to find bucket %s's region not found", bucketDestination[0])
				//_, _ = fmt.Fprintf(os.Stderr, "unable to find bucket %s's region not found\n", bucketDestination[0])
			}
			os.Exit(2)
		}
		targetRegion = S3Region
	}
	return
}

func copyObject(svc *s3.S3, from, to, item, acl string, errChan chan error) {

	_, err := svc.CopyObject(&s3.CopyObjectInput{Bucket: aws.String(to), CopySource: aws.String(from + "/" + item), Key: aws.String(item), ACL: aws.String(acl)})
	if err != nil {
		errChan <- fmt.Errorf("unable to copy %s from bucket %q to bucket %q, %v", item, from, to, err)
		return
	}

	err = svc.WaitUntilObjectExists(&s3.HeadObjectInput{Bucket: aws.String(to), Key: aws.String(item)})
	if err != nil {
		errChan <- fmt.Errorf("error occured while waiting for item %q to be copied in bucket %q, %v", item, to, err)
		return
	}

	errChan <- nil
	return
}

// removeObject : Forward Deletion Event to Bucket DEST
func removeObject(svc *s3.S3, to, item string, errChan chan error) {
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(to),
		Key:    aws.String(item),
	})
	if err != nil {
		errChan <- fmt.Errorf("unable to delete %s in Bucket %q, %v", item, to, err)
		return
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(to),
		Key:    aws.String(item),
	})
	if err != nil {
		errChan <- fmt.Errorf("error occured while waiting for item %q to be removed in bucket %q, %v", item, to, err)
		return
	}

	errChan <- nil
	return
}

func main() {
	lambda.Start(HandleEvent)
}
