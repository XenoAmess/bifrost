package chaosmock_test

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/maximhq/bifrost/tests/cmd/mcpconsole/chaosmock"
)

// TestRealClientReadsToolMetadata: a tool's title, icons and output schema reach a real client as
// the scenario set them, and a tool that sets none arrives without them.
func TestRealClientReadsToolMetadata(t *testing.T) {
	fleet, _ := newFleetWithTools(t,
		chaosmock.Tool{
			Name:         "echo",
			Title:        "Echo",
			Icons:        json.RawMessage(`[{"src":"https://icons.example.invalid/echo.png","mimeType":"image/png","sizes":["48x48"]}]`),
			OutputSchema: json.RawMessage(`{"type":"object","properties":{"echoed":{"type":"string"}},"required":["echoed"]}`),
		},
		chaosmock.Tool{Name: "plain"},
	)
	c, ctx := connectAt(t, fleet.ServerURL("alpha"), "")
	res, err := c.ListTools(ctx, mcp.ListToolsRequest{})
	if err != nil || len(res.Tools) != 2 {
		t.Fatalf("tools/list = %+v, %v", res, err)
	}
	echo, plain := res.Tools[0], res.Tools[1]
	if echo.Title != "Echo" {
		t.Errorf("title = %q, want Echo", echo.Title)
	}
	if len(echo.Icons) != 1 || echo.Icons[0].Src != "https://icons.example.invalid/echo.png" || echo.Icons[0].MIMEType != "image/png" || strings.Join(echo.Icons[0].Sizes, ",") != "48x48" {
		t.Errorf("icons = %+v", echo.Icons)
	}
	if echo.OutputSchema.Type != "object" || echo.OutputSchema.Properties["echoed"] == nil || strings.Join(echo.OutputSchema.Required, ",") != "echoed" {
		t.Errorf("outputSchema = %+v", echo.OutputSchema)
	}
	if plain.Title != "" || plain.Icons != nil || plain.OutputSchema.Type != "" {
		t.Errorf("a tool without metadata must arrive without it, got %+v", plain)
	}
}

// TestToolMetadataIsValidated: icons that are not a list, or an output schema that is not an
// object, are refused rather than served.
func TestToolMetadataIsValidated(t *testing.T) {
	fleet, _ := newFleetWithTools(t)
	for _, body := range []string{
		`{"tools":[{"name":"bad","icons":{"src":"x"}}]}`,
		`{"tools":[{"name":"bad","outputSchema":["string"]}]}`,
	} {
		resp, err := http.Post(fleet.URL()+"/control/alpha/tools/set", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", body, resp.StatusCode)
		}
	}
}

// TestToolReportsProgress: a tool with progress steps reports them, in order and under the caller's
// token, ahead of its result on every transport, and sends none to a call that sent no token.
func TestToolReportsProgress(t *testing.T) {
	fleet, srv := newFleetWithTools(t, chaosmock.Tool{Name: "work", Progress: []chaosmock.ProgressStep{
		{Progress: 1, Total: 2, Message: "step 1 of 2"},
		{Progress: 2, Total: 2},
	}})
	if err := srv.SetProtocolVersions(allVersions); err != nil {
		t.Fatal(err)
	}
	transports := []struct {
		name    string
		connect func() (*client.Client, context.Context)
	}{
		{chaosmock.ProtocolVersion20260728, func() (*client.Client, context.Context) { return connectAt(t, fleet.ServerURL("alpha"), "") }},
		{chaosmock.ProtocolVersion20251125, func() (*client.Client, context.Context) {
			return connectAt(t, fleet.ServerURL("alpha"), chaosmock.ProtocolVersion20251125)
		}},
		{"sse", func() (*client.Client, context.Context) { return connectSSE(t, fleet.SSEURL("alpha")) }},
		{"stdio", func() (*client.Client, context.Context) { return bridgeToClient(t, fleet, "alpha") }},
	}
	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			c, ctx := tr.connect()
			if strings.HasPrefix(tr.name, "20") && c.ProtocolVersion() != tr.name {
				t.Fatalf("negotiated %q, want %q", c.ProtocolVersion(), tr.name)
			}
			var mu sync.Mutex
			var progress []map[string]any
			c.OnNotification(func(n mcp.JSONRPCNotification) {
				if n.Method == string(mcp.MethodNotificationProgress) {
					mu.Lock()
					progress = append(progress, n.Params.AdditionalFields)
					mu.Unlock()
				}
			})

			var call mcp.CallToolRequest
			call.Params.Name = "work"
			call.Params.Meta = &mcp.Meta{ProgressToken: "tok"}
			if _, err := c.CallTool(ctx, call); err != nil {
				t.Fatalf("tools/call: %v", err)
			}
			call.Params.Meta = nil
			if _, err := c.CallTool(ctx, call); err != nil {
				t.Fatalf("tools/call without a token: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			want := []map[string]any{
				{"progressToken": "tok", "progress": float64(1), "total": float64(2), "message": "step 1 of 2"},
				{"progressToken": "tok", "progress": float64(2), "total": float64(2)},
			}
			if !reflect.DeepEqual(progress, want) {
				t.Fatalf("progress = %v, want %v", progress, want)
			}
		})
	}
	if got := srv.Counters().Snapshot().ProgressCalls["work"]; got != int64(len(transports)) {
		t.Fatalf("progress_calls[work] = %d, want %d: only the calls that sent a token", got, len(transports))
	}
}

// TestToolProgressIsValidated: progress that does not increase, or a total below its progress, is refused.
func TestToolProgressIsValidated(t *testing.T) {
	fleet, _ := newFleetWithTools(t)
	for _, body := range []string{
		`{"tools":[{"name":"bad","progress":[{"progress":0}]}]}`,
		`{"tools":[{"name":"bad","progress":[{"progress":2},{"progress":1}]}]}`,
		`{"tools":[{"name":"bad","progress":[{"progress":3,"total":2}]}]}`,
	} {
		resp, err := http.Post(fleet.URL()+"/control/alpha/tools/set", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", body, resp.StatusCode)
		}
	}
}
