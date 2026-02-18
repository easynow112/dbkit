package msg

import (
	"fmt"
)

var Usage = fmt.Sprintf("dbkit <command> [options]\n\nMigration commands:\n  %s\n  %s\n  %s\n\nSeed commands:\n  %s\n  %s", UsageMigrateNew, UsageMigrateUp, UsageMigrateDown, UsageSeed, UsageSeedNew)

const UsageMigrateNew = "dbkit migrate new <name>     Create a new migration"

const UsageMigrateUp = "dbkit migrate up [steps]     Apply pending migrations"

const UsageMigrateDown = "dbkit migrate down [steps]   Roll back applied migrations"

const UsageSeedNew = "dbkit seed new <name>        Create a new seed"

const UsageSeed = "dbkit seed                   Apply all seeds"
