var test = require('tape')
var gjdb = require('../')
var geojsonFixtures = require('@mapbox/geojson-fixtures')
var memdb = require('memdb')
var hyperlog = require('hyperlog')
var path = require('path')
var fdstore = require('fd-chunk-store')

var tmpdir = require('os').tmpdir()
var storefile = path.join(tmpdir, 'gjdb-store-' + Math.random())

var db = gjdb({
  log: hyperlog(memdb('log'), { valueEncoding: 'json' }),
  db: memdb('index'),
  store: fdstore(4096, storefile)
})

var q0 = [ [ 96.954346, 103.914185 ], [ -2.158303, 2.613839 ] ]

Object.keys(geojsonFixtures.feature).forEach(function (f) {
  var gj = geojsonFixtures.feature[f]
  test('query: ' + f, function (t) {
    t.plan(6)
    db.create(gj, function (err, id, node) {
      t.ifError(err, 'no create error')
      t.ok(node, 'created node ok')
      db.ready(function () {
        db.query(q0, function (err, res) {
          t.ifError(err, 'no query error')
          t.deepEqual(res[0].properties, gj.properties)
          t.deepEqual(res[0].geometry, gj.geometry)
          t.equal(res[0].type, 'Feature')
        })
      })
    })
  })
})
