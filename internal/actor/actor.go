// Package actor is the runtime shape every component uses: one goroutine owns
// the component's state and handles one message at a time from its inbox.
//
// Convention for every component (scheduler, agent, guest):
//
//   - State lives in a struct touched only by its own loop goroutine.
//   - The logic is a plain method handle(now time.Time, msg any) []Effect,
//     where Effect is a component-local type (send this message, start this
//     driver call, arm this timer). Unit tests call handle directly with a
//     fake time and assert on the effects; the loop executes them.
//   - Slow work (driver Start/Stop, store commits under the slow-store fault)
//     never runs inside handle. Use Work: it runs fn on a worker goroutine
//     and posts its result back to the inbox as an ordinary message.
//   - HTTP handlers and the SSE hub never read component state. They send a
//     request message carrying a reply channel, or read a published snapshot.
package actor

import "context"

// Run calls handle for each message until ctx is done or inbox is closed.
func Run[M any](ctx context.Context, inbox <-chan M, handle func(M)) {
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-inbox:
			if !ok {
				return
			}
			handle(m)
		}
	}
}

// Work runs fn on a worker goroutine and posts its result to inbox, unless
// ctx ends first. The actor stays responsive (heartbeats keep flowing) while
// fn blocks.
func Work[M any](ctx context.Context, inbox chan<- M, fn func(context.Context) M) {
	go func() {
		m := fn(ctx)
		select {
		case inbox <- m:
		case <-ctx.Done():
		}
	}()
}

// Ask sends a request built around a reply channel and waits for the answer.
// Use it from HTTP handlers to query an actor without sharing its state.
func Ask[M, R any](ctx context.Context, inbox chan<- M, build func(reply chan<- R) M) (R, error) {
	reply := make(chan R, 1)
	var zero R
	select {
	case inbox <- build(reply):
	case <-ctx.Done():
		return zero, ctx.Err()
	}
	select {
	case r := <-reply:
		return r, nil
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}
