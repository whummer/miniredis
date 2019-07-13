// Commands from https://redis.io/commands#generic

package miniredis

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/alicebob/miniredis/v2/geohash"
	"github.com/alicebob/miniredis/v2/server"
)

// commandsGeo handles GEOADD, GEORADIUS etc.
func commandsGeo(m *Miniredis) {
	m.srv.Register("GEOADD", m.cmdGeoAdd)
	m.srv.Register("GEORADIUS", m.cmdGeoRadius)
}

// GEOADD
func (m *Miniredis) cmdGeoAdd(c *server.Peer, cmd string, args []string) {
	if len(args) < 3 || len(args[1:])%3 != 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}
	if m.checkPubsub(c) {
		return
	}
	key, args := args[0], args[1:]

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if db.exists(key) && db.t(key) != "zset" {
			c.WriteError(ErrWrongType.Error())
			return
		}

		toSet := map[string]float64{}
		for len(args) > 2 {
			rawLong, rawLat, name := args[0], args[1], args[2]
			args = args[3:]
			longitude, err := strconv.ParseFloat(rawLong, 64)
			if err != nil {
				c.WriteError("ERR value is not a valid float")
				return
			}
			latitude, err := strconv.ParseFloat(rawLat, 64)
			if err != nil {
				c.WriteError("ERR value is not a valid float")
				return
			}

			if latitude < -85.05112878 ||
				latitude > 85.05112878 ||
				longitude < -180 ||
				longitude > 180 {
				c.WriteError(fmt.Sprintf("ERR invalid longitude,latitude pair %.6f,%.6f", longitude, latitude))
				return
			}

			score := geohash.EncodeIntWithPrecision(latitude, longitude, 52)
			toSet[name] = float64(score)
		}

		set := 0
		for name, score := range toSet {
			if db.ssetAdd(key, score, name) {
				set++
			}
		}
		c.WriteInt(set)
	})
}

type geoRadiusResponse struct {
	Name      string
	Distance  float64
	Longitude float64
	Latitude  float64
}

func (m *Miniredis) cmdGeoRadius(c *server.Peer, cmd string, args []string) {
	if len(args) < 5 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}
	if m.checkPubsub(c) {
		return
	}

	key := args[0]
	longitude, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	latitude, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	radius, err := strconv.ParseFloat(args[3], 64)
	if err != nil || radius < 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	unit := args[4]
	switch unit {
	case "m":
		break
	case "km":
		radius = radius * 1000
	case "mi":
		radius = radius * 1609.34
	case "ft":
		radius = radius * 0.3048
	default:
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}

	var (
		withDist  = false
		withCoord = false
	)
	for _, arg := range args[4:] {
		switch strings.ToUpper(arg) {
		case "WITHCOORD":
			withCoord = true
		case "WITHDIST":
			withDist = true
		}
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)
		members := db.ssetElements(key)

		membersWithinRadius := []geoRadiusResponse{}
		for _, el := range members {
			elLat, elLo := geohash.DecodeIntWithPrecision(uint64(el.score), 52)
			distanceInMeter := distance(latitude, longitude, elLat, elLo)

			if distanceInMeter <= radius {
				membersWithinRadius = append(membersWithinRadius, geoRadiusResponse{
					Name:      el.member,
					Distance:  distanceInMeter,
					Longitude: longitude,
					Latitude:  latitude,
				})
			}
		}

		c.WriteLen(len(membersWithinRadius))
		for _, member := range membersWithinRadius {
			if withDist {
				if withCoord {
					c.WriteLen(3)
				} else {
					c.WriteLen(2)
				}
				c.WriteBulk(member.Name)
				c.WriteBulk(fmt.Sprintf("%f", member.Distance))
			} else {
				if withCoord {
					c.WriteLen(2)
				} else {
					c.WriteLen(1)
				}
				c.WriteBulk(member.Name)
			}

			if withCoord {
				c.WriteLen(2)
				c.WriteBulk(fmt.Sprintf("%f", member.Longitude))
				c.WriteBulk(fmt.Sprintf("%f", member.Latitude))
			}
		}
	})
}

// haversin(θ) function
func hsin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}

// distance function returns the distance (in meters) between two points of
//     a given longitude and latitude relatively accurately (using a spherical
//     approximation of the Earth) through the Haversin Distance Formula for
//     great arc distance on a sphere with accuracy for small distances
//
// point coordinates are supplied in degrees and converted into rad. in the func
//
// distance returned is meters
// http://en.wikipedia.org/wiki/Haversine_formula
// Source: https://gist.github.com/cdipaolo/d3f8db3848278b49db68
func distance(lat1, lon1, lat2, lon2 float64) float64 {
	// convert to radians
	// must cast radius as float to multiply later
	var la1, lo1, la2, lo2, r float64
	la1 = lat1 * math.Pi / 180
	lo1 = lon1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = lon2 * math.Pi / 180

	r = 6378100 // Earth radius in METERS

	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)

	return 2 * r * math.Asin(math.Sqrt(h))
}
