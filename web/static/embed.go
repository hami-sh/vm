// Package static embeds the dashboard: plain HTML, CSS and ES modules with no
// build step. internal/web serves it at /. Owner: W4.
package static

import "embed"

// FS holds index.html and everything under css/ and js/.
//
//go:embed index.html css js
var FS embed.FS
