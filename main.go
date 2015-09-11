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

	svc := dynamodb.New(&aws.Config{Region: aws.String("ap-northeast-1")})

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
			fmt.Println("count :",itemCount," item : ",*items[i]["time"].S)
			itemCount++
		}
		fmt.Println("scan output:",*page.Count,)
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