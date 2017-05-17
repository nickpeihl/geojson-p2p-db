var fixtures = require('@mapbox/geojson-fixtures')
var turf = require('@turf/helpers')

module.exports = {}

var geoms = Object.keys(fixtures.geometry).filter(function (geom) {
  return geom !== 'geometrycollection' && geom !== 'geometrycollection-xyz'
})

geoms.forEach(function (type) {
  var feat = turf.feature(fixtures.geometry[type])
  module.exports[type] = feat
})
