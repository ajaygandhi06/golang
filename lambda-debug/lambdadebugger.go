package lambdadebugger

//#region Import
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go/aws"
)

type messageData struct {
	ContextData string
	EventData   string
}

//#endregion

var (
	functionName   string
	hotsedInRegion string
)

func init() {
	functionName = os.Getenv("AWS_LAMBDA_FUNCTION_NAME")
	hotsedInRegion = os.Getenv("AWS_REGION")

}

// Starts the lambda function in local debug mode
// To debug your function, make sure
// 1. your lambda function's Role has Read\write access to SQS
// 2. Your IAM user account has Read\Write access to SQS
// Parameters:
// handlerFunc: Handler function to handle the lambda execution in local environment
// region: region name where lambda is hosted
// lambdaName: name of lambda function provided by you in AWS environment
func Start(handlerFunc interface{}, region string, lambdaName string) {
	debug := os.Getenv("debug")
	if debug == "true" {
		lambda.Start(handleRequest)
	} else {
		executeInLocalEnvironment(handlerFunc, region, lambdaName)
	}
}

//#region  Local Execution
func executeInLocalEnvironment(handlerFunc interface{}, region string, lambdaName string) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithDefaultRegion(region))
	if err != nil {
		log.Fatalf("Failed to load Configuration, %V", err)
	}
	client := sqs.NewFromConfig(cfg)
	queueUrl := ""
	var receivedData string
	for {
		if queueUrl == "" {
			queueUrl = getQueueUrl(*client, lambdaName)
		}
		if queueUrl != "" {
			receivedData = readMessage(*client, queueUrl)
			if receivedData != "" {
				break
			}
		}
		time.Sleep(10 * time.Second)
	}

	handler := reflect.ValueOf(handlerFunc)
	handlerType := reflect.TypeOf(handlerFunc)
	contextDataType := handlerType.In(0)
	eventDataType := handlerType.In(1)
	contextData := reflect.New(contextDataType)
	eventData := reflect.New(eventDataType)
	messageRecieved := messageData{}
	err = json.Unmarshal([]byte(receivedData), &messageRecieved)
	if err != nil {
		fmt.Println("Error occured while unmarshling event data")
	}

	json.Unmarshal([]byte(messageRecieved.ContextData), contextData.Interface())
	json.Unmarshal([]byte(messageRecieved.EventData), eventData.Interface())

	var args []reflect.Value
	args = append(args, contextData.Elem())
	args = append(args, eventData.Elem())
	handler.Call(args)
}

func getQueueUrl(client sqs.Client, queueName string) string {
	queueUrl := ""
	input := sqs.GetQueueUrlInput{QueueName: aws.String(queueName)}
	urlOutput, err := client.GetQueueUrl(context.TODO(), &input)
	if urlOutput != nil {
		queueUrl = *urlOutput.QueueUrl
	}
	if err != nil {
		fmt.Println(err.Error())
	}
	return queueUrl
}

func readMessage(client sqs.Client, queueUrl string) string {
	message := ""
	msgInput := sqs.ReceiveMessageInput{QueueUrl: aws.String(queueUrl), MaxNumberOfMessages: 1, WaitTimeSeconds: 2}
	msgOutput, _ := client.ReceiveMessage(context.TODO(), &msgInput)
	if len(msgOutput.Messages) > 0 {
		message = *msgOutput.Messages[0].Body
		client.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueUrl),
			ReceiptHandle: msgOutput.Messages[0].ReceiptHandle,
		})

	}
	return message
}

//#endregion

//#region AWS Environment Execution
func handleRequest(ctx context.Context, event interface{}) (string, error) {
	queueUrl := createQueueIfNotExist()

	if queueUrl != "" {
		client := sqs.New(sqs.Options{Region: hotsedInRegion})
		lc, _ := lambdacontext.FromContext(ctx)
		contextData, _ := json.Marshal(lc)
		eventData, _ := json.Marshal(event)
		messageData := messageData{ContextData: string(contextData),
			EventData: string(eventData)}
		dataToSend, _ := json.Marshal(messageData)
		messageSent := sendMessage(*client, queueUrl, string(dataToSend))
		if messageSent {
			return "Execution Succesful", nil
		} else {
			return "Execution failed", errors.New("error in sending message to queue")
		}
	} else {
		return "Execution failed", errors.New("error in creating queue")
	}
}

func createQueueIfNotExist() string {
	queueUrl := ""
	fmt.Println("Checking queue existance")
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithDefaultRegion(hotsedInRegion))
	if err != nil {
		log.Fatalf("Failed to load Configuration, %V", err)
	}
	client := sqs.NewFromConfig(cfg)
	input := sqs.GetQueueUrlInput{QueueName: aws.String(functionName)}
	urlOutput, err := client.GetQueueUrl(context.TODO(), &input)
	if urlOutput == nil {
		fmt.Println(err.Error())
		fmt.Println("Queue doesn't exist. Creating new Queue..")
		policy := "{\"Version\": \"2008-10-17\",\"Id\": \"__default_policy_ID\",\"Statement\": [{\"Sid\": \"__owner_statement\",\"Effect\": \"Allow\",\"Principal\": \"*\",	\"Action\": \"SQS:*\",\"Resource\": \"*\" }] }"
		createQueueInput := sqs.CreateQueueInput{QueueName: aws.String(functionName),
			Attributes: map[string]string{
				"Policy":                 policy,
				"MessageRetentionPeriod": "60",
			}}
		output, createErr := client.CreateQueue(context.TODO(), &createQueueInput)
		if createErr != nil {
			fmt.Println(createErr.Error())
		} else {
			queueUrl = *output.QueueUrl
		}
	} else {
		queueUrl = *urlOutput.QueueUrl
	}
	return queueUrl
}

func sendMessage(client sqs.Client, queueUrl string, data string) bool {
	messageSent := false
	msgInput := sqs.SendMessageInput{MessageBody: aws.String(data), QueueUrl: aws.String(queueUrl)}
	_, err := client.SendMessage(context.TODO(), &msgInput)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		messageSent = true
	}
	return messageSent
}

//#endregion
