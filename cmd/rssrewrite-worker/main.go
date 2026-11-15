// Command rssrewrite-worker manages host-shared RSS URL rewrite rules (ruTorrent rssurlrewrite parity).
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

type rule struct {
	Name        string `json:"name"`
	Pattern     string `json:"pattern"`
	Replacement string `json:"replacement"`
	Enabled     bool   `json:"enabled"`
}

var (
	mu       sync.Mutex
	nextHost = int64(1)
	pending  = map[int64]chan hostReply{}
	outMu    sync.Mutex
)

type hostReply struct {
	result json.RawMessage
	err    string
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for in.Scan() {
		line := append([]byte(nil), in.Bytes()...)
		if len(line) == 0 {
			continue
		}
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(line, &probe); err != nil {
			continue
		}
		if _, hasMethod := probe["method"]; !hasMethod {
			var resp struct {
				ID     any             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(line, &resp); err != nil {
				continue
			}
			idNum, ok := asInt64(resp.ID)
			if !ok {
				continue
			}
			mu.Lock()
			ch := pending[idNum]
			delete(pending, idNum)
			mu.Unlock()
			if ch == nil {
				continue
			}
			msg := ""
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			ch <- hostReply{result: resp.Result, err: msg}
			continue
		}

		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		// Concurrent so hostCall reverse-RPC can complete while stdin keeps reading replies.
		go handleRequest(req)
	}
}

func handleRequest(req request) {
	switch req.Method {
	case "fleet.handshake", "fleet.ping", "ext.activate", "ext.deactivate", "ext.health", "ext.upgrade":
		reply(req.ID, map[string]any{"ok": true})
	case "fleet.shutdown":
		reply(req.ID, map[string]any{"ok": true})
		os.Exit(0)
	case "ext.rssrewrite.getRules":
		rules, err := listRules()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, map[string]any{
			"rules":     rules,
			"rulesText": rulesText(rules),
		})
	case "ext.rssrewrite.saveRules":
		var p struct {
			RulesText string `json:"rulesText"`
			Rules     []rule `json:"rules"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			replyErr(req.ID, "invalid rules")
			return
		}
		var rules []rule
		if strings.TrimSpace(p.RulesText) != "" {
			parsed, err := parseRulesText(p.RulesText)
			if err != nil {
				replyErr(req.ID, err.Error())
				return
			}
			rules = parsed
		} else {
			rules = p.Rules
		}
		if err := saveRules(rules); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		saved, err := listRules()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, map[string]any{"ok": true, "rules": saved, "rulesText": rulesText(saved)})
	case "ext.rssrewrite.clearRules":
		if err := saveRules(nil); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, map[string]any{"ok": true, "rules": []rule{}, "rulesText": ""})
	case "ext.rssrewrite.apply":
		var p struct {
			URL string `json:"url"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil || strings.TrimSpace(p.URL) == "" {
			replyErr(req.ID, "url required")
			return
		}
		out, changed, matched, err := applyURL(p.URL)
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, map[string]any{"url": out, "changed": changed, "matchedRule": matched})
	default:
		replyErr(req.ID, fmt.Sprintf("method not found: %s", req.Method))
	}
}

func listRules() ([]rule, error) {
	raw, err := hostCall("fleet.urlrewrite.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var res struct {
		Rules []rule `json:"rules"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, err
	}
	if res.Rules == nil {
		res.Rules = []rule{}
	}
	return res.Rules, nil
}

func saveRules(rules []rule) error {
	if rules == nil {
		rules = []rule{}
	}
	_, err := hostCall("fleet.urlrewrite.set", map[string]any{"rules": rules})
	return err
}

func applyURL(u string) (string, bool, string, error) {
	raw, err := hostCall("fleet.urlrewrite.apply", map[string]any{"url": u})
	if err != nil {
		return "", false, "", err
	}
	var res struct {
		URL         string `json:"url"`
		Changed     bool   `json:"changed"`
		MatchedRule string `json:"matchedRule"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		return "", false, "", err
	}
	return res.URL, res.Changed, res.MatchedRule, nil
}

func rulesText(rules []rule) string {
	lines := make([]string, 0, len(rules))
	for _, r := range rules {
		flag := "off"
		if r.Enabled {
			flag = "on"
		}
		lines = append(lines, r.Name+"|"+r.Pattern+"|"+r.Replacement+"|"+flag)
	}
	return strings.Join(lines, "\n")
}

func parseRulesText(text string) ([]rule, error) {
	out := make([]rule, 0)
	seen := map[string]struct{}{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 4)
		if len(parts) < 3 {
			return nil, fmt.Errorf("expected Name|pattern|replacement[|on|off] on line %q", line)
		}
		name := strings.TrimSpace(parts[0])
		pattern := strings.TrimSpace(parts[1])
		replacement := parts[2]
		enabled := true
		if len(parts) == 4 {
			flag := strings.ToLower(strings.TrimSpace(parts[3]))
			switch flag {
			case "", "on", "1", "true", "yes":
				enabled = true
			case "off", "0", "false", "no":
				enabled = false
			default:
				return nil, fmt.Errorf("enabled flag must be on/off on line %q", line)
			}
		}
		if name == "" || pattern == "" {
			return nil, fmt.Errorf("empty name or pattern on line %q", line)
		}
		key := strings.ToLower(name)
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("duplicate rule name %q", name)
		}
		seen[key] = struct{}{}
		out = append(out, rule{
			Name:        name,
			Pattern:     pattern,
			Replacement: replacement,
			Enabled:     enabled,
		})
	}
	return out, nil
}

func hostCall(method string, params any) (json.RawMessage, error) {
	ch := make(chan hostReply, 1)
	mu.Lock()
	id := nextHost
	nextHost++
	pending[id] = ch
	mu.Unlock()

	b, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      id,
	})
	if err != nil {
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, err
	}
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()

	select {
	case reply := <-ch:
		if reply.err != "" {
			return nil, fmt.Errorf("%s", reply.err)
		}
		if len(reply.result) == 0 {
			return json.RawMessage("null"), nil
		}
		return reply.result, nil
	case <-time.After(30 * time.Second):
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, fmt.Errorf("host call timeout")
	}
}

func reply(id any, result any) {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "result": result, "id": id})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func replyErr(id any, msg string) {
	b, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"error":   map[string]any{"code": -32000, "message": msg},
		"id":      id,
	})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}
