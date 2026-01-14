package main

import (
    "encoding/json"
    "log"
    "context"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// using mutexes will also work since each node only handles it's own key in kv

func main() {
    n := maelstrom.NewNode()
    kv := maelstrom.NewSeqKV(n)
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    n.Handle("add", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        delta := int(body["delta"].(float64))

        for {
            val, err := kv.ReadInt(ctx, n.ID())
            if err != nil {
                err = kv.CompareAndSwap(ctx, n.ID(), 0, delta, true)
                if err == nil {
                    break
                }
                continue
            }
            
            newVal := val + delta
            err = kv.CompareAndSwap(ctx, n.ID(), val, newVal, false)
            if err == nil {
                break
            }
        }
        
        resp := map[string]any{
            "type": "add_ok",
        }

        return n.Reply(msg, resp)
    })

    n.Handle("read", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        res := int(0)

        for _, node := range n.NodeIDs() {
            val, err := kv.ReadInt(ctx, node)
            if err != nil {
                continue
            }
            res += val
        }

        resp := map[string]any{
            "type": "read_ok",
            "value": res,
        }

        return n.Reply(msg, resp)
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }

}
