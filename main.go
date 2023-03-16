package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/luis-ale-117/cella"
)

var client *azqueue.ServiceClient
var development bool

func main() {
	log.Println("Starting worker...")
	cer := cella.Cell(0)
	log.Println(cer)

	// Handle SIGINT
	sigchnl := make(chan os.Signal, 1)
	signal.Notify(sigchnl, os.Interrupt, syscall.SIGINT)
	go func() {
		<-sigchnl
		log.Println()
		log.Println("SIGINT received, exiting")
		if development {
			log.Println("Deleting queue in development mode")
			_, err := client.DeleteQueue(context.TODO(), "testqueue", nil)
			if err != nil {
				log.Printf("Error deleting queue %s", err)
			}
			log.Println("Queue deleted")
		}
		os.Exit(0)
	}()

	// Steps using the minimal version:
	// 1. Keep asking the Queue for new unfinished processes
	// 2. When a task is found, execute it if system is not too busy
	// 3. Each gen save its compressed file to the DB and update its state

	// Keep asking the queue for the next task
	development = os.Getenv("ENV") == "DEV"
	var connectionString string
	var queueName string
	if development {
		log.Println("Development mode")
		connectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
		queueName = "testqueue"

	} else {
		log.Println("Production mode")
		connectionString = os.Getenv("QUEUE_CONNECTION_STRING")
		queueName = os.Getenv("QUEUE_NAME")
	}

	var err error
	for {
		client, err = azqueue.NewServiceClientFromConnectionString(connectionString, nil)
		if err != nil {
			log.Printf("Can not create service client %s, waiting %v seconds", err, 5)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("Service client created %v", client)
		break
	}

	pager := client.NewListQueuesPager(&azqueue.ListQueuesOptions{
		Include: azqueue.ListQueuesInclude{Metadata: true},
	})
	// Create queue if in development mode
	for pager.More() {
		resp, err := pager.NextPage(context.Background())
		if err != nil {
			log.Fatalf("Error getting page %s", err)
		}
		if len(resp.Queues) == 0 && development {
			log.Println("No queues found")
			_, err = client.CreateQueue(context.TODO(), queueName, nil)
			if err != nil {
				log.Fatalf("Error creating queue %s", err)
			}
			// Delete queue after use
			defer func() {
				_, err = client.DeleteQueue(context.TODO(), "testqueue", nil)
				if err != nil {
					log.Printf("Error deleting queue %s", err)
				}
				log.Println("Queue deleted")
			}()
			log.Println("Queue created")
		}
	}

	queueClient := client.NewQueueClient(queueName)
	// Enqueue a test message
	_, err = queueClient.EnqueueMessage(context.Background(), "Hello World!", &azqueue.EnqueueMessageOptions{VisibilityTimeout: to.Ptr(int32(0))})
	if err != nil {
		log.Fatalf("Error enqueuing message %s", err)
	}
	for {
		if queueClient == nil {
			log.Printf("Queue unavailable, waiting %v seconds", 5)
			time.Sleep(5 * time.Second)
			continue
		}
		// Dequeue a message
		respDeque, err := queueClient.DequeueMessages(context.Background(), &azqueue.DequeueMessagesOptions{VisibilityTimeout: to.Ptr(int32(31))})
		if err != nil {
			log.Printf("Error dequeuing messages %s, waiting %v seconds", err, 5)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("Length of dequeued messages: %v", len(respDeque.Messages))
		if len(respDeque.Messages) == 0 {
			log.Printf("No messages in the queue, waiting %v seconds", 10)
			time.Sleep(10 * time.Second)
			continue
		}
		message := respDeque.Messages[0]

		log.Printf("Dequeue: %v", *message.MessageText)
		log.Printf("ID: %v", *message.MessageID)
		log.Printf("Pop receipt: %v", *message.PopReceipt)
		log.Printf("Insertion time: %v", *message.InsertionTime)
		log.Printf("Expiration time: %v", *message.ExpirationTime)
		log.Printf("Time next visible: %v", *message.TimeNextVisible)
		log.Printf("Dequeue count: %v", *message.DequeueCount)

		// TODO: Use thread pool to execute tasks
		go processMessage(message, queueClient)
		// TODO: Process messages by connecting to database and getting data
		// TODO: Save data to file
		// TODO: Compress file
		// TODO: Save file to DB
		// TODO: Update process state
		// TODO: Delete message
		// TODO: Repeat

	}

}

func processMessage(message *azqueue.DequeuedMessage, client *azqueue.QueueClient) error {
	// Process message
	log.Printf("Processing message in processMessage %v", *message.MessageText)
	update := true
	go HandleMessage(message, client, &update)
	// Simulate processing
	log.Println("Processing...")
	time.Sleep(5 * time.Second)
	log.Println("Processing done!!!")
	// Stop updating message
	update = false

	return nil
}

func HandleMessage(message *azqueue.DequeuedMessage, client *azqueue.QueueClient, update *bool) {
	var err error

	for *update {
		msgUpdate := fmt.Sprintf("Updated %v", *message.MessageText)
		options := &azqueue.UpdateMessageOptions{VisibilityTimeout: to.Ptr(int32(30))}
		time.Sleep(30 * time.Second)
		// Update message visibility timeout to 60 seconds
		updatedMessage, err := client.UpdateMessage(context.Background(), *message.MessageID, *message.PopReceipt, msgUpdate, options)
		if err != nil {
			log.Printf("Error updating message %s", err)
		}
		message.PopReceipt = updatedMessage.PopReceipt
		log.Println("Message updated using HandleMessage")
	}
	// Delete message
	_, err = client.DeleteMessage(context.Background(), *message.MessageID, *message.PopReceipt, nil)
	if err != nil {
		log.Printf("Error deleting message %s", err)
	}
	log.Println("Message deleted")
}

// PROGRAM STEPS
// 1. Connect to queue
// 2. Create queue (dev)
// 3. Poll messages to start processing
// 4. Process messages by connecting to database and getting data
// 5. Start processing messages and store data locally
// 6. Update message until processing is complete
// 7. Save data to database
// 8. Delete message
