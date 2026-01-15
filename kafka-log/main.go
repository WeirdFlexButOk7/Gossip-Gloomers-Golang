package main

import (
    "encoding/json"
    "log"
    "context"
    "strconv"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
    n := maelstrom.NewNode()
    kv := maelstrom.NewLinKV(n)
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    n.Handle("send", func(msg maelstrom.Message) error {
        var body map[string]any
        var offset int
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        key := body["key"].(string)
        readKey := key + "log"
        msgVal := int(body["msg"].(float64))

        for {
            raw, err := kv.Read(ctx, readKey)
            if err != nil {
                num, err := strconv.Atoi(key[0:])
                if err != nil {
                    return err
                }
                num *= 100
                offset = num
                init := []any{
                    []any{num, msgVal},
                }
                if kv.CompareAndSwap(ctx, readKey, nil, init, true) == nil {
                    break
                }
                continue
            }
            
            rawArr := raw.([]any)
            last := rawArr[len(rawArr)-1].([]any)
            offset = int(last[0].(float64)) + 1

            newEntry := []any{offset, msgVal}
            newRaw := append(rawArr, newEntry)

            if kv.CompareAndSwap(ctx, readKey, rawArr, newRaw, false) == nil {
                break
            }

        }
        
        resp := map[string]any{
            "type": "send_ok",
            "offset": offset,
        }

        return n.Reply(msg, resp)
    })

    n.Handle("poll", func(msg maelstrom.Message) error {
        var body map[string]any
        msgs := make(map[string][][]int)

        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        for key, off := range body["offsets"].(map[string]any) {
            readKey := key + "log"
            offsetLimit := int(off.(float64))
            initArr := [][]int{}

            raw, err := kv.Read(ctx, readKey)
            if err != nil {
                msgs[key] = [][]int{}
                continue
            }

            kvVals := raw.([]any)

            for _, value := range kvVals {
                valueArr := value.([]any)
                offset := int(valueArr[0].(float64))
                msg := int(valueArr[1].(float64))

                if offset >= offsetLimit {
                    initArr = append(initArr, []int{offset, msg})
                }
            }
            msgs[key] = initArr
        }

        resp := map[string]any{
            "type": "poll_ok",
            "msgs": msgs,
        }

        return n.Reply(msg, resp)
    })

    n.Handle("commit_offsets", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        for key, off := range body["offsets"].(map[string]any) {
            offset := int(off.(float64))
            offKey := key + "off"

            for {
                cur, err := kv.ReadInt(ctx, offKey)
                if err != nil {
                    if kv.CompareAndSwap(ctx, offKey, nil, offset, true) == nil {
                        break
                    }
                    continue
                }
                if kv.CompareAndSwap(ctx, offKey, cur, max(cur, offset), false) == nil {
                    break
                }
            }
            
        }

        resp := map[string]any{
            "type": "commit_offsets_ok",
        }

        return n.Reply(msg, resp)
    })

    n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
        var body map[string]any
        initMap := map[string]int{}

        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        keys := body["keys"].([]any)

        for _, k := range keys {
            key := k.(string)
            readKey := key + "off"

            maxVal, err := kv.ReadInt(ctx, readKey)
            if err != nil {
                continue
            }

            initMap[key] = maxVal
        }

        resp := map[string]any{
            "type": "list_committed_offsets_ok",
            "offsets": initMap,
        }

        return n.Reply(msg, resp)
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }

}
