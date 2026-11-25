package mcp

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/maximhq/bifrost/core/schemas"
)

// ProgressReporter receives the progress an upstream reports for one tools/call. The MCP gateway puts
// one on ctx under BifrostContextKeyMCPProgressReporter when its caller sent a progressToken.
type ProgressReporter func(progress float64, total *float64, message string)

// progressRoute is one in-flight call's reporter. Once stopped, it reports nothing more.
type progressRoute struct {
	mu      sync.Mutex
	stopped bool
	report  ProgressReporter
}

// progressRoutes maps each progress token Bifrost sent upstream to the call that sent it.
// Tokens are random, so one upstream cannot report progress on another's call.
var progressRoutes sync.Map

// withProgressToken asks the upstream for progress when ctx carries a reporter, and returns the call to
// send with a func that stops routing its progress. Without a reporter, call is returned unchanged.
func withProgressToken(ctx context.Context, call mcp.CallToolRequest) (mcp.CallToolRequest, func()) {
	report, _ := ctx.Value(schemas.BifrostContextKeyMCPProgressReporter).(ProgressReporter)
	if report == nil {
		return call, func() {}
	}
	token := uuid.NewString()
	meta := &mcp.Meta{ProgressToken: token}
	if call.Params.Meta != nil {
		meta.AdditionalFields = call.Params.Meta.AdditionalFields
	}
	call.Params.Meta = meta
	route := &progressRoute{report: report}
	progressRoutes.Store(token, route)
	return call, func() {
		progressRoutes.Delete(token)
		// Waits out a delivery in flight, so no progress reaches the caller after its result.
		route.mu.Lock()
		route.stopped = true
		route.mu.Unlock()
	}
}

// relayProgress hands conn's progress notifications to the call whose token they carry.
func relayProgress(conn *client.Client) {
	conn.OnNotification(func(n mcp.JSONRPCNotification) {
		if n.Method == string(mcp.MethodNotificationProgress) {
			deliverProgress(n.Params.AdditionalFields)
		}
	})
}

// deliverProgress reports one notifications/progress to the call that holds its token. Progress for
// a token no call holds, or without a numeric progress, is dropped.
func deliverProgress(params map[string]any) {
	token, _ := params["progressToken"].(string)
	value, ok := progressRoutes.Load(token)
	if !ok {
		return
	}
	progress, ok := params["progress"].(float64)
	if !ok {
		return
	}
	var total *float64
	if t, ok := params["total"].(float64); ok {
		total = &t
	}
	message, _ := params["message"].(string)
	route := value.(*progressRoute)
	route.mu.Lock()
	defer route.mu.Unlock()
	if !route.stopped {
		route.report(progress, total, message)
	}
}
