package eav2

import (
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

type a string

var attrs = [...]a{
	"artist",
	"album",
	"day",
	"month",
	"year",
	"track",
	"order",
	"duration",
}

var attrOrdinals = func() map[a]Ordinal {
	ret := make(map[a]Ordinal, len(attrs))
	for i, v := range attrs {
		ret[v] = Ordinal(i + 1)
	}
	return ret
}()

func (a a) String() string {
	return string("a")
}

func (a a) Ordinal() Ordinal {
	ord, ok := attrOrdinals[a]
	if !ok {
		panic(errors.AssertionFailedf("unknown attribute %s", a))
	}
	return ord
}

var _ Attribute = a("")

func TestMusicInfo(t *testing.T) {
	sc := NewSchema(Mappings{
		TypeMappings: map[reflect.Type]TypeMappings{
			reflect.TypeOf((*Artist)(nil)): {
				"Name": a("artist"),
			},
			reflect.TypeOf((*Album)(nil)): {
				"Name":              a("album"),
				"Artist":            a("artist"),
				"ReleaseDate.Day":   a("day"),
				"ReleaseDate.Month": a("month"),
				"ReleaseDate.Year":  a("year"),
			},
			reflect.TypeOf((*Track)(nil)): {
				"Artist":   a("artist"),
				"Album":    a("album"),
				"Name":     a("track"),
				"Order":    a("order"),
				"Duration": a("duration"),
			},
		},
	})
	tr := NewTree(sc, nil)
	for _, d := range []interface{}{
		&Beatles,
		&RollingStones,
		&PleasePleaseMe,
	} {
		require.Nil(t, tr.Insert(d))
	}
	v := sc.MakeValues(Map{
		a("artist"): ArtistName("The Beatles!"),
	})
	_ = tr.Iterate(v, EntityIteratorFunc(func(entity Entity) error {
		t.Log(entity)
		return nil
	}))
}
