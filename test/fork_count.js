var test = require('tape')
var hyperlog = require('hyperlog')
var fdstore = require('fd-chunk-store')
var path = require('path')
var memdb = require('memdb')
var fixtures = require('./fixtures')

var tmpdir = require('os').tmpdir()
var storefile0 = path.join(tmpdir, 'osm-store-' + Math.random())
var storefile1 = path.join(tmpdir, 'osm-store-' + Math.random())

var osmdb = require('../')

test('count forks', function (t) {
  t.plan(10)
  var osm0 = osmdb({
    log: hyperlog(memdb(), { valueEncoding: 'json' }),
    db: memdb(),
    store: fdstore(4096, storefile0)
  })
  var osm1 = osmdb({
    log: hyperlog(memdb(), { valueEncoding: 'json' }),
    db: memdb(),
    store: fdstore(4096, storefile1),
    size: 4096
  })
  var docs = {
    'point': fixtures['point'],
    'point-xyz': fixtures['point-xyz'],
    'multilinestring': fixtures['multilinestring']
  }
  var names = {}
  var nodes = {}
  var versions = {}

  var keys = Object.keys(docs).sort()
  ;(function next () {
    if (keys.length === 0) return ready()
    var key = keys.shift()
    var doc = docs[key]
    if (doc.refs) {
      doc.refs = doc.refs.map(function (ref) { return names[ref] })
    }
    osm0.create(doc, function (err, k, node) {
      t.ifError(err)
      names[key] = k
      if (!versions[key]) versions[key] = []
      versions[key].push(node.key)
      nodes[k] = node
      next()
    })
  })()

  function ready () {
    var r0 = osm0.log.replicate()
    var r1 = osm1.log.replicate()
    r0.pipe(r1).pipe(r0)
    r0.once('end', function () {
      var newdoc0 = {
        type: 'Feature',
        geometry: {
          coordinates: [101, 1],
          type: 'Point'
        },
        properties: {}
      }
      var newdoc1 = {
        type: 'Feature',
        geometry: {
          coordinates: [101.5, 1.5],
          type: 'Point'
        },
        properties: {}
      }
      osm0.put(names['point'], newdoc0, function (err, node) {
        t.ifError(err)
        versions['point'].push(node.key)

        osm1.put(names['point'], newdoc1, function (err, node) {
          t.ifError(err)
          versions['point'].push(node.key)
          replicate()
        })
      })
    })
  }

  function replicate () {
    var r0 = osm0.log.replicate()
    var r1 = osm1.log.replicate()
    r0.pipe(r1).pipe(r0)
    r0.once('end', function () {
      check()
    })
  }

  function check () {
    var expected = {}
    expected[names['multilinestring']] = 1
    expected[names['point-xyz']] = 1
    expected[names['point']] = 2
    osm0.kv.createReadStream({ values: false }).on('data', function (row) {
      t.equal(row.links.length, expected[row.key], 'expected fork count')
    })
    osm0.get(names['point'], function (err, values) {
      t.ifError(err)
      var expected = {}
      expected[versions['point'][1]] = {
        type: 'Feature',
        id: names['point'],
        geometry: {
          coordinates: [101, 1],
          type: 'Point'
        },
        properties: {}
      }
      expected[versions['point'][2]] = {
        type: 'Feature',
        id: names['point'],
        geometry: {
          coordinates: [101.5, 1.5],
          type: 'Point'
        },
        properties: {}
      }
      t.deepEqual(values, expected, 'expected fork values')
    })
  }
})
