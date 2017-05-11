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

Object.keys(geojsonFixtures.feature).forEach(function (f) {
  var gj = geojsonFixtures.feature[f]
  test('put: ' + f, function (t) {
    t.plan(5)
    db.put(f, gj, function (err) {
      t.ifError(err)
      db.get(f, function (err, data) {
        t.ifError(err)
        var key = Object.keys(data)[0]
        t.equal(data[key].id, f)
        t.deepEqual(data[key].properties, gj.properties)
        t.deepEqual(data[key].geometry, gj.geometry)
      })
    })
  })
})
