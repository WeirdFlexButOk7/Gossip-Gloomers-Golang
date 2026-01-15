package main

import (
    "encoding/json"
    "log"
    "context"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// g0, g1a, g1b, g1c in this commit
// similar to how mvcc works.

func main() {
    n := maelstrom.NewNode()
    kv := maelstrom.NewSeqKV(n)
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    n.Handle("txn", func(msg maelstrom.Message) error {
        var body map[string]any
        local := make(map[string]int)

        errResp := map[string]any{
            "type": "error",
            "code": maelstrom.TxnConflict,
            "text": "txn abort",
        }

        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return n.Reply(msg, errResp)
        }

        resp := map[string]any{
            "type": "txn_ok",
            "txn": [][]any{},
        }

        txns := body["txn"].([]any)
        out := resp["txn"].([][]any)

        for _, rawArr := range txns {
            arr := rawArr.([]any)
            op := arr[0].(string)
            key := string(int(arr[1].(float64)))

            if op == "w" {
                // kv.Write(ctx, key, intValue);
                intValue := int(arr[2].(float64))
                local[key] = intValue
            } else {
                // intValue, _ := kv.ReadInt(ctx, key)
                intValue, ok := local[key]
                if !ok {
                    intValue, _ = kv.ReadInt(ctx, key)
                }
                arr[2] = intValue
            }

            out = append(out, arr)
        }

        resp["txn"] = out

        for key, value := range local {
            // kv.Write(ctx, key, value)
            for {
                cur, err := kv.ReadInt(ctx, key)
                if err != nil {
                    if kv.CompareAndSwap(ctx, key, nil, value, true) == nil {
                        break
                    }
                    continue
                }
                if kv.CompareAndSwap(ctx, key, cur, value, false) == nil {
                    break
                }
            }
        }
        return n.Reply(msg, resp)
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }

}
