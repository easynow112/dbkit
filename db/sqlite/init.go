package sqlite

import (
	"github.com/easynow112/dbkit/db"
	_ "modernc.org/sqlite"
)

func init() {
	db.RegisterDriver("sqlite", NewDB)
}
