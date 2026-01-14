package main

import (
    "encoding/json"
    "log"
    "time"
    "sync"
    "fmt"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// inspiration form snowflake unique id generator

type Container struct {
    mu sync.Mutex
    counter int64
    prevMicros int64
    start time.Time
}

func (c *Container) inc() int64 {
    c.mu.Lock();
    defer c.mu.Unlock();

    micros := time.Since(c.start).Microseconds()

    if c.prevMicros == micros {
        c.counter++
        if c.counter == (1 << 15) {
            for c.prevMicros == micros {
                micros = time.Since(c.start).Microseconds()
            }
            c.counter = 0
            c.prevMicros = micros
        }
    } else {
        c.counter = 0
        c.prevMicros = micros
    }

    id := (micros << 15) | c.counter

    return id;

}

func main() {
    n := maelstrom.NewNode()
    c := Container{
        counter: int64(0),
        prevMicros: int64(0),
        start: time.Now(),
    }

    n.Handle("generate", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        body["type"] = "generate_ok"
        body["id"] = n.ID() + fmt.Sprintf("%d", c.inc())
        return n.Reply(msg, body)
    })
    
    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}
