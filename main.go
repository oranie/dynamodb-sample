package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
	"fmt"
	"time"
)

func main() {
	t := time.Now()
	testTableCreate()

	svc := dynamodb.New(&aws.Config{Region: aws.String("ap-northeast-1")})

	createResp,createErr := testTableCreate()
	if createErr != nil {
		fmt.Println(createErr.Error())
		return
	}
	log.Println("create table",createResp)

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
		for i := 0; i < len(items); i++   {
			//fmt.Println("count :",itemCount," item : ",*items[i]["time"].S)
			itemCount++
		}
		fmt.Println("scan output:",*page.Count,)
		fmt.Println(page.LastEvaluatedKey)
		return page.LastEvaluatedKey != nil
	})

	if err != nil {
		fmt.Println(err.Error())
		return
	}

    log.Println("-------scan end--------")
	log.Println("start time:",t)
	log.Println("end time:",time.Now())
}

func testTableCreate() (*dynamodb.CreateTableOutput,error) {
	svc := dynamodb.New(&aws.Config{Region: aws.String("ap-northeast-1")})

	params := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{ // Required
			{ // Required
				AttributeName: aws.String("key"), // Required
				AttributeType: aws.String("S"),    // Required
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{ // Required
			{ // Required
				AttributeName: aws.String("key"), // Required
				KeyType:       aws.String("HASH"),                // Required
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
	testTableDelete()

	resp, err := svc.CreateTable(params)

	if err != nil {
		panic(err.Error())
	}

	fmt.Println(resp)
	return resp,err
}

func testTableDelete() (){
	svc := dynamodb.New(&aws.Config{Region: aws.String("ap-northeast-1")})

	params := &dynamodb.DeleteTableInput{
		TableName: aws.String("result"), // Required
	}
	resp, err := svc.DeleteTable(params)

	if err != nil {
		panic(err.Error())
		return
	}

	fmt.Println(resp)
}