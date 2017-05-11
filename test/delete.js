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
  test('delete: ' + f, function (t) {
    t.plan(5)
    db.create(gj, function (err, id, node) {
      t.ifError(err)
      t.ok(node)
      db.del(id, function (err, node) {
        t.ifError(err)
        t.ok(node)
        t.equal(node.value.d, id)
      })
    })
  })
})
