// Package static embeds the dashboard: plain HTML, CSS and JS with no build
// step. internal/web serves it at /. Owner: W4.
package static

import "embed"

// FS holds index.html, app.js and style.css.
//
//go:embed index.html app.js style.css
var FS embed.FS
