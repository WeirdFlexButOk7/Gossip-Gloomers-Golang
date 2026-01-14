package main

import (
    "encoding/json"
    "log"
    "sync"
    "fmt"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Container struct {
    mu          sync.Mutex
    messages    []int
    topology    map[string][]string
    uids        map[string]struct{}
}

func (c *Container) Set(message int) {
    c.mu.Lock();
    defer c.mu.Unlock();

    c.messages = append(c.messages, message)
}

func (c *Container) SetTop(topology map[string][]string) {
    c.mu.Lock();
    defer c.mu.Unlock();

    c.topology = topology
}

func (c *Container) Get() []int {
    c.mu.Lock();
    defer c.mu.Unlock();

    return c.messages
}

func (c *Container) GetTop(node string) []string {
    c.mu.Lock();
    defer c.mu.Unlock();

    return c.topology[node]
}

func (c *Container) NotPresent(uid string) bool {
    c.mu.Lock();
    defer c.mu.Unlock();

    if _, ok := c.uids[uid]; ok {
        return false
    }

    c.uids[uid] = struct{}{}
    return true
}

func main() {
    n := maelstrom.NewNode()
    c := Container{
        messages: []int{},
        topology: map[string][]string{},
        uids: map[string]struct{}{},
    }

    n.Handle("broadcast", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        c.Set(int(body["message"].(float64)))

        sendNode := map[string]any{
            "type": "store",
            "message": body["message"],
            "uid": n.ID() + fmt.Sprintf("%d", int(body["message"].(float64))),
        }

        n.Send(n.ID(), sendNode);

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
            "messages": c.Get(),
        }

        return n.Reply(msg, resp)
    })

    n.Handle("topology", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        rawTopo := body["topology"].(map[string]any)
        topo := make(map[string][]string)

        for node, neigh := range rawTopo {
            neighSlice := neigh.([]any)
            neighbors := make([]string, 0, len(neighSlice))
            for _, n := range neighSlice {
                neighbors = append(neighbors, n.(string))
            }
            topo[node] = neighbors
        }

        c.SetTop(topo)

        resp := map[string]any{
            "type": "topology_ok",
        }

        return n.Reply(msg, resp)
    })

    n.Handle("store", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }
        
        if c.NotPresent(body["uid"].(string)) {
            c.Set(int(body["message"].(float64)))
            for _, node := range c.GetTop(n.ID()) {
                n.Send(node, body)
            }
        }

        return nil
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}
/*

n0 - n3 - n4 - n1 - n2
n0 - n1

*/
