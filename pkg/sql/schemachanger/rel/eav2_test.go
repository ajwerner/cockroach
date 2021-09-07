package rel

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type ArtistName string
type AlbumName string

type Year int
type Day int
type TrackOrder int

type Date struct {
	Day   Day
	Month time.Month
	Year  Year
}

type Artist struct {
	Name ArtistName
}

type Album struct {
	Name        AlbumName
	Artist      ArtistName
	ReleaseDate Date
}

type Track struct {
	Artist   ArtistName
	Album    AlbumName
	Name     string
	Order    TrackOrder
	Duration time.Duration
}

var (
	Beatles       = Artist{Name: "The Beatles!"}
	RollingStones = Artist{Name: "The Rolling Stones"}

	PleasePleaseMe = Album{
		Name:        "Please Please Me",
		Artist:      Beatles.Name,
		ReleaseDate: Date{Day: 22, Month: time.March, Year: 1963},
	}
)

type A string

var attrs = [...]A{
	"artist",
	"album",
	"day",
	"month",
	"year",
	"track",
	"order",
	"duration",
}

var attrOrdinals = func() map[A]Ordinal {
	ret := make(map[A]Ordinal, len(attrs))
	for i, v := range attrs {
		ret[v] = Ordinal(i + 1)
	}
	return ret
}()

func (a A) String() string { return string(a) }

func (a A) Ordinal() Ordinal {
	ord, ok := attrOrdinals[a]
	if !ok {
		panic(errors.AssertionFailedf("unknown attribute %s", a))
	}
	return ord
}

var _ Attribute = A("")

func TestMusicInfo(t *testing.T) {
	sc := MustSchema("", Mappings{
		TypeMappings: map[reflect.Type]map[string]Attribute{
			reflect.TypeOf((*Artist)(nil)): {
				"Name": A("artist"),
			},
			reflect.TypeOf((*Album)(nil)): {
				"Name":              A("album"),
				"Artist":            A("artist"),
				"ReleaseDate.Day":   A("day"),
				"ReleaseDate.Month": A("month"),
				"ReleaseDate.Year":  A("year"),
			},
			reflect.TypeOf((*Track)(nil)): {
				"Artist":   A("artist"),
				"Album":    A("album"),
				"Name":     A("track"),
				"Order":    A("order"),
				"Duration": A("duration"),
			},
		},
	})
	db := NewDatabase(sc, nil)
	for _, d := range []interface{}{
		&Beatles,
		&RollingStones,
		&PleasePleaseMe,
	} {
		require.Nil(t, db.Insert(d))
	}

	var a Var = "a"
	q, err := NewQuery(sc,
		a.Attr(A("artist"), Value(ArtistName("The Beatles!"))),
		Filter("a")(func(artist *Artist) bool {
			fmt.Println("hi", artist)
			return true
		}),
	)
	require.Nil(t, err)
	require.NoError(t, q.Prepare().Iterate(db, func(r Result) error {
		v := r.Var(a)
		_, err := fmt.Printf("%T %v\n", v, v)
		return err
	}))
}
