package pg

import (
	"github.com/easynow112/dbkit/db"
)

func init() {
	db.RegisterDriver("pg", NewDB)
}
