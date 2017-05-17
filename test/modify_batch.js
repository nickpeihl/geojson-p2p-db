var test = require('tape')
var hyperlog = require('hyperlog')
var fdstore = require('fd-chunk-store')
var path = require('path')
var memdb = require('memdb')
var collect = require('collect-stream')
var fixtures = require('./fixtures')

var tmpdir = require('os').tmpdir()
var storefile = path.join(tmpdir, 'osm-store-' + Math.random())

var osmdb = require('../')

test('del batch', function (t) {
  t.plan(14)
  var osm = osmdb({
    log: hyperlog(memdb(), { valueEncoding: 'json' }),
    db: memdb(),
    store: fdstore(4096, storefile)
  })
  var batch0 = Object.keys(fixtures).filter(function (key) {
    return key === 'point' || key === 'multipoint'
  }).map(function (key) {
    return {
      type: 'put', key: key, value: fixtures[key]
    }
  })
  var batch1 = Object.keys(fixtures).filter(function (key) {
    return key === 'multilinestring'
  }).map(function (key) {
    return {
      type: 'put', key: key, value: fixtures[key]
    }
  }).concat([ { type: 'del', key: 'point' } ])
  var versions = {}
  osm.batch(batch0, function (err, nodes) {
    t.error(err)
    nodes.forEach(function (node) {
      versions[node.value.k] = node.key
    })
    osm.batch(batch1, function (err, nodes) {
      t.error(err)
      nodes.forEach(function (node) {
        versions[node.value.k || node.value.d] = node.key
      })
      ready()
    })
  })
  function ready () {
    var q0 = [[-1.0, 2.0], [99.0, 102.0]]
    var ex0 = [{
      deleted: true,
      id: 'point',
      version: versions['point']
    }, {
      type: 'Feature',
      id: 'multipoint',
      geometry: {
        coordinates: [ [100.0, 0.0], [101.0, 1.0] ],
        type: 'MultiPoint'
      },
      properties: {},
      version: versions['multipoint']
    }, {
      type: 'Feature',
      id: 'multilinestring',
      geometry: {
        coordinates: [
          [ [100.0, 0.0], [101.0, 1.0] ],
          [ [102.0, 2.0], [103.0, 3.0] ]
        ],
        type: 'MultiLineString',
      },
      properties: {},
      version: versions['multilinestring']
    }].sort(idcmp)
    osm.query(q0, function (err, res) {
      t.error(err)
      t.deepEqual(res.sort(idcmp), ex0, 'full coverage query')
    })
    collect(osm.queryStream(q0), function (err, res) {
      t.error(err)
      t.deepEqual(res.sort(idcmp), ex0, 'full coverage stream')
    })
    var q1 = [[-1.5, 1.5], [100.5, 102.0]]
    var ex1 = [{
      type: 'Feature',
      id: 'multipoint',
      geometry: {
        coordinates: [ [100.0, 0.0], [101.0, 1.0] ],
        type: 'MultiPoint'
      },
      properties: {},
      version: versions['multipoint']
    }, {
      type: 'Feature',
      id: 'multilinestring',
      geometry: {
        coordinates: [
          [ [100.0, 0.0], [101.0, 1.0] ],
          [ [102.0, 2.0], [103.0, 3.0] ]
        ],
        type: 'MultiLineString'
      },
      properties: {},
      version: versions['multilinestring']
    }].sort(idcmp)
    osm.query(q1, function (err, res) {
      t.error(err)
      t.deepEqual(res.sort(idcmp), ex1, 'partial coverage query')
    })
    collect(osm.queryStream(q1), function (err, res) {
      t.error(err)
      t.deepEqual(res.sort(idcmp), ex1, 'partial coverage stream')
    })
    var q2 = [[-1.5, 1.5], [98.5, 99.5]]
    var ex2 = []
    osm.query(q2, function (err, res) {
      t.error(err)
      t.deepEqual(res.sort(idcmp), ex2, 'empty coverage query')
    })
    collect(osm.queryStream(q2), function (err, res) {
      t.error(err)
      t.deepEqual(res.sort(idcmp), ex2, 'empty coverage stream')
    })
  }
})

function idcmp (a, b) {
  return a.id < b.id ? -1 : 1
}
