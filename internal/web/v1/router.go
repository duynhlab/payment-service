// Package v1 holds the payment service web layer (Gin handlers + routing).
//
// TODO: P1 web layer lands in the next commit — this file only defines the
// route-registration surface so cmd/main.go compiles against it.
package v1

import "github.com/gin-gonic/gin"

// Handler is the payment web-layer handler. Placeholder for now: the P1
// handlers (authorize/capture/void/refund + idempotency) attach here in the
// next commit.
type Handler struct{}

// RegisterRoutes mounts the payment v1 routes on the engine. Stub: no routes
// are registered yet — the P1 web layer lands in the next commit.
func RegisterRoutes(r *gin.Engine, h *Handler) {
	_ = r
	_ = h
}
