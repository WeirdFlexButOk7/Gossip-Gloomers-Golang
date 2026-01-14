package main

import (
    "encoding/json"
    "log"
    "sync"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Container struct {
    mu sync.Mutex
    messages []int
}

func (c *Container) set(message int) {
    c.mu.Lock();
    defer c.mu.Unlock();

    c.messages = append(c.messages, message)
}

func (c *Container) get() []int {
    c.mu.Lock();
    defer c.mu.Unlock();

    return c.messages
}

func main() {
    n := maelstrom.NewNode()
    c := Container{
        messages : []int{},
    }

    n.Handle("broadcast", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        c.set(int(body["message"].(float64)))

        resp := map[string]any{
            "type": "broadcast_ok",
        }

        return n.Reply(msg, resp)
    })

    n.Handle("read", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        resp := map[string]any{
            "type": "read_ok",
            "messages": c.get(),
        }

        return n.Reply(msg, resp)
    })

    n.Handle("topology", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        resp := map[string]any{
            "type": "topology_ok",
        }

        return n.Reply(msg, resp)
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}
