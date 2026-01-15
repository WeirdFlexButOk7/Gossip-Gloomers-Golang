package main

import (
    "encoding/json"
    "log"
    "context"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// not handling G0 anomaly, test is broken, planning to handle in challenge 6c

func main() {
    n := maelstrom.NewNode()
    kv := maelstrom.NewSeqKV(n)
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    n.Handle("txn", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
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
                intValue := int(arr[2].(float64))
                kv.Write(ctx, key, intValue);
            } else {
                intValue, _ := kv.ReadInt(ctx, key)
                arr[2] = intValue
            }

            out = append(out, arr)

        }

        resp["txn"] = out

        return n.Reply(msg, resp)
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }

}
