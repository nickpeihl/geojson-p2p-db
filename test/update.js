var test = require('tape')
var hyperlog = require('hyperlog')
var fdstore = require('fd-chunk-store')
var path = require('path')
var memdb = require('memdb')
var fixtures = require('./fixtures')

var tmpdir = require('os').tmpdir()
var storefile = path.join(tmpdir, 'osm-store-' + Math.random())

var osmdb = require('../')

test('update node', function (t) {
  t.plan(8)
  var osm = osmdb({
    log: hyperlog(memdb(), { valueEncoding: 'json' }),
    db: memdb(),
    store: fdstore(4096, storefile)
  })
  var docs = {
    'point': fixtures['point'],
    'multipoint': fixtures['multipoint'],
    'polygon': fixtures['polygon']
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
    osm.create(doc, function (err, k, node) {
      t.ifError(err)
      names[key] = k
      versions[key] = node.key
      nodes[k] = node
      next()
    })
  })()

  function ready () {
    var newdoc = {
      type: 'Feature',
      geometry: {
        coordinates: [101, 1],
        type: 'Point'
      },
      properties: {}
    }
    osm.put(names['point'], newdoc, function (err, node) {
      t.ifError(err)
      versions['point'] = node.key
      check()
    })
  }

  function check () {
    var q0 = [[-1.0, 2.0], [99.0, 102.0]]
    var ex0 = [{
      type: 'Feature',
      geometry: {
        coordinates: [101, 1],
        type: 'Point'
      },
      properties: {},
      id: names['point'],
      version: versions['point']
    }, {
      type: 'Feature',
      geometry: {
        type: 'MultiPoint',
        coordinates: [ [100.0, 0.0], [101.0, 1.0] ]
      },
      properties: {},
      id: names['multipoint'],
      version: versions['multipoint']
    }, {
      type: 'Feature',
      geometry:  {
        type: 'Polygon',
        coordinates: [
          [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
        ]
      },
      properties: {},
      id: names['polygon'],
      version: versions['polygon']
    }].sort(idcmp)
    osm.query(q0, function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex0, 'updated query 0')
    })
    var q1 = [[-1.5, 1.5], [100.5, 102.0]]
    var ex1 = [{
      type: 'Feature',
      geometry: {
        coordinates: [101, 1],
        type: 'Point'
      },
      properties: {},
      id: names['point'],
      version: versions['point']
    }, {
      type: 'Feature',
      geometry: {
        type: 'MultiPoint',
        coordinates: [ [100.0, 0.0], [101.0, 1.0] ]
      },
      properties: {},
      id: names['multipoint'],
      version: versions['multipoint']
    }, {
      type: 'Feature',
      geometry:  {
        type: 'Polygon',
        coordinates: [
          [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
        ]
      },
      properties: {},
      id: names['polygon'],
      version: versions['polygon']
    }].sort(idcmp)
    osm.query(q1, function (err, res) {
      t.ifError(err)
      t.deepEqual(res.sort(idcmp), ex1, 'updated query 1')
    })
  }
})

function idcmp (a, b) {
  return a.id < b.id ? -1 : 1
}
