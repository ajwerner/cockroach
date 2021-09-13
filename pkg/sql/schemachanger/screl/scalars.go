package screl

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

func GetDescID(e scpb.Element) descpb.ID {
	id, err := Schema.GetAttribute(DescID, e)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(
			err, "failed to retrieve descriptor ID for %T", e,
		))
	}
	return *id.(*descpb.ID)
}
