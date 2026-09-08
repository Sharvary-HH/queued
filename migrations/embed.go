// Package migrations carries the SQL files as an embedded filesystem so the
// binaries can migrate themselves without the .sql files being deployed
// alongside them.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
