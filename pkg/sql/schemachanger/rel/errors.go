package rel

import "github.com/cockroachdb/errors"

func catchError(err *error) {
	var rErr error
	switch r := recover().(type) {
	case nil:
		return
	case error:
		rErr = errors.Wrap(r, "failed to construct query")
	default:
		rErr = errors.AssertionFailedf("failed to construct query: %v", r)
	}
	if *err == nil {
		*err = rErr
	} else {
		*err = errors.CombineErrors(*err, rErr)
	}
}
