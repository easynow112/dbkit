package fs

import (
	"github.com/easynow112/dbkit/source"
)

func init() {
	source.RegisterDriver("fs", NewFSSourceStore)
}
