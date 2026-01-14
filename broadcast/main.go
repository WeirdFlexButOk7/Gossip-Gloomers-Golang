package main

import (
    "encoding/json"
    "log"
    "sync"
    "fmt"
    "time"

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

func replicate(n *maelstrom.Node, uid string, value int, node string) {
    cnt := int(2)
    for {
        body := map[string]any{
            "type": "store",
            "uid": uid,
            "message": value,
            "parent": n.ID(),
        }

        done := make(chan struct{})

        n.RPC(node, body, func(msg maelstrom.Message) error {
            close(done)
            return nil
        })

        select {
        case <-done:
            return
        case <-time.After(time.Duration(cnt) * time.Second):
            cnt++
            log.Printf("Retry replicating %d to %s", value, node)
        }
    }
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

        // n.RPC(n.ID(), sendNode, func(msg maelstrom.Message) error {
            // return nil
        // })
        
        message := int(body["message"].(float64))
        uid := fmt.Sprintf("%d-", int(body["msg_id"].(float64))) + fmt.Sprintf("%d", message);

        if c.NotPresent(uid) {
            log.Printf("[%s] Storing NEW message %d (uid=%s)", n.ID(), message, uid)
            c.Set(message)
            for _, node := range c.GetTop(n.ID()) {
                go replicate(n, uid, message, node)
            }
        } else {
            log.Printf("[%s] DUPLICATE message %d (uid=%s)", n.ID(), message, uid)
        }

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

        for node, _ := range rawTopo {
            // neighSlice := _.([]any)
            // neighbors := make([]string, 0, len(neighSlice))
            // for _, n := range neighSlice {
            //     neighbors = append(neighbors, n.(string))
            // }
            // topo[node] = neighbors

            if node != "n0" {
                topo[node] = []string{"n0"}
                topo["n0"] = append(topo["n0"], node)
            }
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
                if node != body["parent"].(string) {
                    go replicate(n, body["uid"].(string), int(body["message"].(float64)), node)
                }
            }
        }

        resp := map[string]any{
            "type": "store_ok",
        }

        return n.Reply(msg, resp)
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }

}
/*

n0 - n3 - n4 - n1 - n2
n0 - n1

*/
