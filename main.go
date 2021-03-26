package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/go-redis/redis/v8"
)

type PSubscriber struct {
	client *redis.Client
}

var ctx = context.Background()
var conn = redis.NewClient(&redis.Options{
	Addr:     "127.0.0.1:6379",
	Password: "",
})

func main() {
	log.Println("======== start =========")
	p := PSubscriber{client: conn}
	// enable expired event notifier on redis.
	p.client.Do(ctx, "CONFIG", "SET", "notify-keyspace-events", "Ex")
	// main listener for expired events .
	p.Listen(ctx, "__key*__:expired")

}

func (r *PSubscriber) Listen(ctx context.Context, pattern string) {
	events := r.client.PSubscribe(ctx, pattern)
	for {
		log.Println("Waiting for expired event ... ")
		msg, err := events.ReceiveMessage(ctx)
		if msg != nil && err == nil {
			// split keys into 2 part
			key := strings.Split(msg.Payload, ":")
			// get actual value from expired event
			val := r.client.Get(ctx, key[1])
			// DO SOMETHING WITH EXPIRED VALUE
			fmt.Printf("key : %v ", msg.Payload)
			fmt.Println("value :", val)
			// DELETE ACTUAL VALUE
			r.client.Del(ctx, key[1])
		}
		if err != nil {
			fmt.Println("Error : ", err)
		}
	}
}
