package kvtoypb

//go:generate go run -tags gen-batch gen_batch.go

func (ba *BatchRequest) IsReadOnly() bool {
	return len(ba.Requests) > 0 && !ba.hasFlags(isWrite)
}

func (ba *BatchRequest) hasFlags(flags flagSet) bool {
	for _, union := range ba.Requests {
		if (union.GetInner().flags() & flags) != 0 {
			return true
		}
	}
	return false
}

// hasFlagForAll returns true iff all of the requests within the batch contains
// the specified flag.
func (ba *BatchRequest) hasFlagForAll(flags flagSet) bool {
	if len(ba.Requests) == 0 {
		return false
	}
	for _, union := range ba.Requests {
		if (union.GetInner().flags() & flags) == 0 {
			return false
		}
	}
	return true
}
