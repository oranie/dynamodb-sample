package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"log"
	"time"
)

type Record struct {
	Key       string
	RemoteID  string
	OtherData map[string]int
	Timestamp int64
}

type NginxLog struct {
	Id            string
	Time          string
	BodyBytesSent string
	BytesSent     string
	ForWardedFor  string
	QueryString   string
	Referer       string
	RemoteAddr    string
	RequestLength string
	RequestMethod string
	RequestTime   string
	RequestUri    string
	Status        string
	Tag           string
	Useragent     string
}

func main() {
	//testTableDelete()
	//chack table status

	//testTableCreate()
	//chack table status
	//if creating,wait for active

	svc := dynamodb.New(&aws.Config{Region: aws.String("ap-northeast-1")})
	/*
		scanParams := &dynamodb.ScanInput{
			TableName:aws.String("access_log_range"),
			AttributesToGet:[]*string{
				aws.String("id"),
				aws.String("time"),
				aws.String("body_bytes_sent"),
				aws.String("bytes_sent"),
				aws.String("forwardedfor"),
				aws.String("query_string"),
				aws.String("referer"),
				aws.String("remote_addr"),
				aws.String("request_length"),
				aws.String("request_method"),
				aws.String("request_time"),
				aws.String("request_uri"),
				aws.String("status"),
				aws.String("tag"),
				aws.String("useragent"),
			},
			//Limit: aws.Int64(1000000),
		}
	*/

	r := Record{
		Key:       "key127.0.0.1",
		RemoteID:  "abc-001",
		OtherData: map[string]int{"a": 1, "b": 2, "c": 3},
		Timestamp: time.Now().UTC().Unix(),
	}
	item, err := dynamodbattribute.ConvertToMap(r)
	log.Println(item)

	result, err := svc.PutItem(&dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String("result"),
	})

	fmt.Println(result, err)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		return
	}
	fmt.Println(result)

}

func testScanTable() {
	t := time.Now()
	svc := dynamodb.New(&aws.Config{Region: aws.String("ap-northeast-1")})
	scanParams := &dynamodb.ScanInput{
		TableName: aws.String("result"),
	}

	resp, err := svc.Scan(scanParams)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	log.Println(resp.LastEvaluatedKey)
	pageNum := 0
	itemCount := 0
	err = svc.ScanPages(scanParams, func(page *dynamodb.ScanOutput, lastPage bool) bool {
		pageNum++
		items := page.Items
		for i := 0; i < len(items); i++ {
			fmt.Println("count :", itemCount, " item : ", *items[i]["time"].S)
			itemCount++
		}
		fmt.Println("scan output:", *page.Count)
		fmt.Println(page.LastEvaluatedKey)
		return page.LastEvaluatedKey != nil
	})

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	log.Println("-------scan end--------")
	log.Println("start time:", t)
	log.Println("end time:", time.Now())
}

func testTableCreate() (*dynamodb.CreateTableOutput, error) {
	svc := dynamodb.New(&aws.Config{Region: aws.String("ap-northeast-1")})

	params := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{ // Required
			{ // Required
				AttributeName: aws.String("Key"), // Required
				AttributeType: aws.String("S"),   // Required
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{ // Required
			{ // Required
				AttributeName: aws.String("Key"),  // Required
				KeyType:       aws.String("HASH"), // Required
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{ // Required
			ReadCapacityUnits:  aws.Int64(100), // Required
			WriteCapacityUnits: aws.Int64(100), // Required
		},
		TableName: aws.String("result"), // Required
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String("NEW_AND_OLD_IMAGES"),
		},
	}

	resp, err := svc.CreateTable(params)

	if err != nil {
		log.Println(err.Error())
	}

	fmt.Println(resp)
	return resp, err
}

func testTableDelete() {
	svc := dynamodb.New(&aws.Config{Region: aws.String("ap-northeast-1")})

	params := &dynamodb.DeleteTableInput{
		TableName: aws.String("result"), // Required
	}
	resp, err := svc.DeleteTable(params)

	if err != nil {
		log.Println(err.Error())
		return
	}

	fmt.Println(resp.TableDescription.TableStatus)
}
