// +build int

package main

// GEO keys.

import (
	"testing"
)

func TestGeo(t *testing.T) {
	testCommands(t,
		succ("GEOADD",
			"Sicily",
			"13.361389", "38.115556", "Palermo",
			"15.087269", "37.502669", "Catania",
		),
		succ("GEOADD",
			"mountains",
			"86.9248308", "27.9878675", "Everest",
			"142.1993050", "11.3299030", "Challenger Deep",
			"31.132", "29.976", "Pyramids",
		),
		succ("GEOADD", // re-add an existing one
			"mountains",
			"86.9248308", "27.9878675", "Everest",
		),
		succ("GEOADD", // update
			"mountains",
			"86.9248308", "28.000", "Everest",
		),

		// failure cases
		fail("GEOADD", "err", "186.9248308", "27.9878675", "not the Everest"),
		fail("GEOADD", "err", "-186.9248308", "27.9878675", "not the Everest"),
		fail("GEOADD", "err", "86.9248308", "87.9878675", "not the Everest"),
		fail("GEOADD", "err", "86.9248308", "-87.9", "not the Everest"),
		succ("SET", "str", "I am a string"),
		fail("GEOADD", "str", "86.9248308", "27.9878675", "Everest"),
		fail("GEOADD"),
		fail("GEOADD", "foo"),
		fail("GEOADD", "foo", "86.9248308"),
		fail("GEOADD", "foo", "86.9248308", "27.9878675"),
		succ("GEOADD", "foo", "86.9248308", "27.9878675", ""),
		fail("GEOADD", "foo", "eight", "27.9878675", "bar"),
		fail("GEOADD", "foo", "86.9248308", "seven", "bar"),
	)
}
