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

test('fork', function (t) {
  t.plan(12)
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
    'point-xyz': fixtures['point-xyz']
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
    var q0 = [[-1.0, 2.0], [99.0, 102.0]]
    var ex0 = [
      {
        type: 'Feature',
        properties: {},
        geometry: {
          type: 'Point',
          coordinates: [100, 0, 1]
        },
        id: names['point-xyz'],
        version: versions['point-xyz'][0]
      },
      {
        type: 'Feature',
        geometry: {
          coordinates: [101, 1],
          type: 'Point'
        },
        properties: {},
        id: names['point'],
        version: versions['point'][1]
      },
      {
        type: 'Feature',
        geometry: {
          coordinates: [101.5, 1.5],
          type: 'Point'
        },
        properties: {},
        id: names['point'],
        version: versions['point'][2]
      }
    ].sort(idcmp)
    osm0.query(q0, function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex0, 'updated query 0')
    })
    osm1.query(q0, function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex0, 'updated query 0')
    })
    var q1 = [[-1.5, 2.5], [99.5, 102.5]]
    var ex1 = [
      {
        type: 'Feature',
        properties: {},
        geometry: {
          type: 'Point',
          coordinates: [100, 0, 1]
        },
        id: names['point-xyz'],
        version: versions['point-xyz'][0]
      },
      {
        type: 'Feature',
        geometry: {
          coordinates: [101, 1],
          type: 'Point'
        },
        properties: {},
        id: names['point'],
        version: versions['point'][1]
      },
      {
        type: 'Feature',
        geometry: {
          coordinates: [101.5, 1.5],
          type: 'Point'
        },
        properties: {},
        id: names['point'],
        version: versions['point'][2]
      }
    ].sort(idcmp)
    osm0.query(q1, function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex1, 'updated query 1')
    })
    osm1.query(q1, function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex1, 'updated query 1')
    })
  }
})

function idcmp (a, b) {
  var aloc = a.geometry.coordinates[1] + ',' + a.geometry.coordinates[0]
  var bloc = b.geometry.coordinates[1] + ',' + b.geometry.coordinates[0]
  if (a.id === b.id) return aloc < bloc ? -1 : 1
  return a.id < b.id ? -1 : 1
}
