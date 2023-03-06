package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/luis-ale-117/cella"
)

func main() {
	log.Println("Starting worker...")
	cer := cella.Cell(0)
	log.Println(cer)

	// Steps using the minimal version:
	// 1. Keep asking the DB for new unfinished processes
	// 2. When a task is found, execute it if system is not too busy
	// 3. Each gen save its compressed file to the DB and update its state

	// Keep asking the queue for the next task
	development := os.Getenv("DEVELOPMENT")

	if development == "true" {
		log.Println("Development mode")
	} else {
		development = "false"
		log.Println("Production mode")
	}
	connectionString := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"

	client, err := azqueue.NewServiceClientFromConnectionString(connectionString, nil)
	if err != nil {
		log.Fatalf("Can not create service client %s", err)
	}
	pager := client.NewListQueuesPager(&azqueue.ListQueuesOptions{
		Include: azqueue.ListQueuesInclude{Metadata: true},
	})
	// list pre-existing queues, create if needed
	for pager.More() {
		resp, err := pager.NextPage(context.Background())
		if err != nil {
			log.Fatalf("Error getting page %s", err)
		}
		if len(resp.Queues) == 0 {
			log.Println("No queues found")
			_, err = client.CreateQueue(context.TODO(), "testqueue", &azqueue.CreateOptions{
				Metadata: map[string]*string{"hello": to.Ptr("world")},
			})
			if err != nil {
				log.Fatalf("Error creating queue %s", err)
			}
			// Delete queue after use
			defer func() {
				_, err = client.DeleteQueue(context.TODO(), "testqueue", nil)
				if err != nil {
					log.Fatalf("Error deleting queue %s", err)
				}
				log.Println("Queue deleted")
			}()
			log.Println("Queue created")
		}
	}

	pager = client.NewListQueuesPager(&azqueue.ListQueuesOptions{
		Include: azqueue.ListQueuesInclude{Metadata: true},
	})
	// list pre-existing queues
	for pager.More() {
		resp, err := pager.NextPage(context.Background())
		if err != nil {
			log.Fatalf("Error getting page %s", err)
		}
		if len(resp.Queues) == 0 {
			log.Fatalln("No queues found")
		}
		for _, _queue := range resp.Queues {
			log.Printf("Queue %v", *_queue.Name)
			queueClient := client.NewQueueClient(*_queue.Name)
			for i := 0; i < 3; i++ {
				msg := fmt.Sprintf("(%s message %v)", *_queue.Name, i)
				optEnqueue := &azqueue.EnqueueMessageOptions{VisibilityTimeout: to.Ptr(int32(0))}
				_, err := queueClient.EnqueueMessage(context.Background(), msg, optEnqueue)
				if err != nil {
					log.Fatalf("Error enqueueing message %s", err)
				}
				log.Println("Message enqueued: ", msg)
			}
			opts := &azqueue.PeekMessagesOptions{NumberOfMessages: to.Ptr(int32(3))}
			resp2, err := queueClient.PeekMessages(context.Background(), opts)
			if err != nil {
				log.Fatalf("Error peeking messages %s", err)
			}
			// check 3 messages retrieved
			log.Println("Number peeked ", len(resp2.Messages))
			for i, message := range resp2.Messages {
				log.Printf("Peeking %v: %v", i, *message.MessageText)
			}

			// Dequeue messages
			optsDeque := &azqueue.DequeueMessagesOptions{NumberOfMessages: to.Ptr(int32(3)), VisibilityTimeout: to.Ptr(int32(1))}
			resp3, err := queueClient.DequeueMessages(context.Background(), optsDeque)
			if err != nil {
				log.Fatalf("Error dequeuing messages %s", err)
			}
			// check 3 messages retrieved
			log.Println(len(resp3.Messages))
			for i, message := range resp3.Messages {
				log.Printf("Dequeue %v: %v", i, *message.MessageText)
				log.Printf("ID %v: %v", i, *message.MessageID)
				log.Printf("Pop receipt %v: %v", i, *message.PopReceipt)
				// Insertion time
				log.Printf("Insertion time %v: %v", i, *message.InsertionTime)
				// Expiration time
				log.Printf("Expiration time %v: %v", i, *message.ExpirationTime)
				// Time next visible
				log.Printf("Time next visible %v: %v", i, *message.TimeNextVisible)
				// Dequeue count
				log.Printf("Dequeue count %v: %v", i, *message.DequeueCount)

				// Update message
				msgUpdate := fmt.Sprintf("Updated %v", *message.MessageText)
				options := &azqueue.UpdateMessageOptions{VisibilityTimeout: to.Ptr(int32(10))}
				_, err := queueClient.UpdateMessage(context.Background(), *message.MessageID, *message.PopReceipt, msgUpdate, options)
				if err != nil {
					log.Fatalf("Error updating message %s", err)
				}
				log.Println("Message updated")
			}

			// Sleep for 2 seconds
			log.Println("Sleep for 2 seconds")
			time.Sleep(2 * time.Second)
			// Redequeue messages
			log.Println("Redequeue messages")
			optsDeque = &azqueue.DequeueMessagesOptions{NumberOfMessages: to.Ptr(int32(3))}
			resp3, err = queueClient.DequeueMessages(context.Background(), optsDeque)
			if err != nil {
				log.Fatalf("Error dequeuing messages %s", err)
			}
			// check 3 messages retrieved
			log.Println("Redequeue messages ", len(resp3.Messages))
			for i, message := range resp3.Messages {
				log.Printf("Dequeue %v: %v", i, *message.MessageText)
				log.Printf("ID %v: %v", i, *message.MessageID)
				log.Printf("Pop receipt %v: %v", i, *message.PopReceipt)
				// Insertion time
				log.Printf("Insertion time %v: %v", i, *message.InsertionTime)
				// Expiration time
				log.Printf("Expiration time %v: %v", i, *message.ExpirationTime)
				// Time next visible
				log.Printf("Time next visible %v: %v", i, *message.TimeNextVisible)
				// Dequeue count
				log.Printf("Dequeue count %v: %v", i, *message.DequeueCount)
			}

			// Sleep for 2 seconds
			log.Println("Sleep for 5 seconds")
			time.Sleep(5 * time.Second)
			// Redequeue messages
			optsDeque = &azqueue.DequeueMessagesOptions{NumberOfMessages: to.Ptr(int32(3)), VisibilityTimeout: to.Ptr(int32(1))}
			resp3, err = queueClient.DequeueMessages(context.Background(), optsDeque)
			if err != nil {
				log.Fatalf("Error dequeuing messages %s", err)
			}
			// check 3 messages retrieved
			log.Println("Redequeue messages finally", len(resp3.Messages))
			for i, message := range resp3.Messages {
				log.Printf("Dequeue %v: %v", i, *message.MessageText)
				log.Printf("ID %v: %v", i, *message.MessageID)
				log.Printf("Pop receipt %v: %v", i, *message.PopReceipt)
				// Insertion time
				log.Printf("Insertion time %v: %v", i, *message.InsertionTime)
				// Expiration time
				log.Printf("Expiration time %v: %v", i, *message.ExpirationTime)
				// Time next visible
				log.Printf("Time next visible %v: %v", i, *message.TimeNextVisible)
				// Dequeue count
				log.Printf("Dequeue count %v: %v", i, *message.DequeueCount)
			}

			// Delete messages
			for _, message := range resp3.Messages {
				_, err := queueClient.DeleteMessage(context.Background(), *message.MessageID, *message.PopReceipt, nil)
				if err != nil {
					log.Fatalf("Error deleting message %s", err)
				}
				log.Println("Message deleted")
			}

			// Peek messages again
			resp4, err := queueClient.PeekMessages(context.Background(), opts)
			if err != nil {
				log.Fatalf("Error peeking messages %s", err)
			}
			// check 0 messages retrieved
			log.Println("Message peeked: ", len(resp4.Messages))

		}
	}
}
