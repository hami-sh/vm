// Package guest is guestd's protocol and loop: retry Register until accepted,
// then Heartbeat. An authenticated rejection stops the loop. The same code
// runs in the sim and in cmd/guestd. Owner: W2.
package guest
