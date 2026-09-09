// Package web carries the dashboard's templates and static files as an embedded
// filesystem, the same way migrations does, so the binary is self-contained and
// there is no "did you remember to copy web/" step in a deployment.
package web

import (
	"embed"
	"io/fs"
)

//go:embed all:templates
var templateFS embed.FS

//go:embed all:static
var staticEmbedFS embed.FS

// Templates holds the html/template sources.
var Templates fs.FS = templateFS

// Static is rooted at the static directory so it can be mounted at /static/.
var Static = mustSub(staticEmbedFS, "static")

func mustSub(f fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		panic(err)
	}
	return sub
}
