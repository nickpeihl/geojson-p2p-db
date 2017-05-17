var test = require('tape')
var hyperlog = require('hyperlog')
var fdstore = require('fd-chunk-store')
var path = require('path')
var memdb = require('memdb')
var collect = require('collect-stream')
var xtend = require('xtend')
var fixtures = require('./fixtures')

var tmpdir = require('os').tmpdir()
var storefile = path.join(tmpdir, 'osm-store-' + Math.random())

var osmdb = require('../')

test('batch put all geojson geometry types', function (t) {
  t.plan(13)
  var osm = osmdb({
    log: hyperlog(memdb(), { valueEncoding: 'json' }),
    db: memdb(),
    store: fdstore(4096, storefile)
  })
  var rows = Object.keys(fixtures).map(function (key) {
    return {
      type: 'put', key: key, value: fixtures[key]
    }
  })
  osm.batch(rows, function (err, nodes) {
    t.error(err)
    var q0 = [[-1.0, 2.0], [99.0, 102.0]]
    var ex0 = Object.keys(fixtures).map(function (key, i) {
      var doc = xtend(fixtures[key], {
        id: key,
        version: nodes[i].key
      })
      return doc
    }).sort(idcmp)

    osm.query(q0, function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex0, 'full coverage query')
    })
    collect(osm.queryStream(q0), function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex0, 'full coverage stream')
    })
    var q1 = [[-1.5, 1.5], [100.5, 102.0]]
    var ex1 = Object.keys(fixtures).map(function (key, i) {
      var doc = xtend(fixtures[key], {
        id: key,
        version: nodes[i].key
      })
      return doc
    }).filter(function (feat) {
      return feat.id !== 'point' && feat.id !== 'point-xyz'
    }).sort(idcmp)
    osm.query(q1, function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex1, 'partial coverage query')
    })
    collect(osm.queryStream(q1), function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex1, 'partial coverage stream')
    })
    var q2 = [[-1.5, 1.5], [98.5, 99.5]]
    var ex2 = []
    osm.query(q2, function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex2, 'empty coverage query')
    })
    collect(osm.queryStream(q2), function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex2, 'empty coverage stream')
    })
  })
})

function idcmp (a, b) {
  return a.id < b.id ? -1 : 1
}
