package apperrors

import (
	"fmt"
	"strings"
)

type InvalidArgs struct {
	Args []string
	Hint string
}

func (e *InvalidArgs) Error() string {
	return fmt.Sprintf("Invalid arguments: %s", strings.Join(e.Args, " "))
}
