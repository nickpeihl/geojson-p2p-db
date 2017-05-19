/* global L */

var hyperlog = require('hyperlog')
var level = require('level-browserify')
var idbstore = require('idb-chunk-store')
var randomBytes = require('randombytes')
var sampleFeatures = require('./sample.json')
var turf = require('@turf/helpers')
var sublevel = require('subleveldown')

var map = L.map('map', {
  center: [48.5, -123.0],
  zoom: 11
})
L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
  attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
}).addTo(map)

var gjLayer = L.geoJSON()
    .bindPopup(function (layer) {
      return layer.feature.id
    })
    .addTo(map)

map.addControl(new L.Control.Draw({
  edit: {
    featureGroup: gjLayer,
    poly: {
      allowIntersection: false
    }
  }
}))

var db = level('gjdb')

var gjdb = require('../')
window.gj = gjdb({
  log: hyperlog(sublevel(db, 'log'), { valueEncoding: 'json' }),
  db: sublevel(db, 'index'),
  store: idbstore(4096)
})

initMapListeners()

window.gj.ready(function () {
  var features = sampleFeatures.features;
  (function next () {
    if (features.length === 0) return ready()
    var feat = features.shift()
    var key = feat.id ? feat.id : randomBytes(8).toString('hex')
    feat.id = key
    window.gj.get(key, function (err, node) {
      if (err) throw err
      if (Object.keys(node).length === 0) {
        console.log('feature does not exist yet')
        window.gj.put(key, feat, function (err) {
          if (err) throw err
          next()
        })
      } else {
        console.log('feature exists')
        next()
      }
    })
  })()
})

function ready () {
  window.gj.log.heads(function (err, heads) {
    if (err) throw err
    var fc = turf.featureCollection(heads.filter(function (head) {
      // Don't return deleted features or null values
      return !head.value.d && head.value.v
    }).map(function (head) {
      // Return geojson value of head
      return head.value.v
    }))
    gjLayer.clearLayers()
    gjLayer.addData(fc)
    map.flyToBounds(gjLayer.getBounds())
  })
}

function initMapListeners () {
  map.on('draw:created', function (e) {
    var feat = e.layer.toGeoJSON()
    var key = randomBytes(8).toString('hex')
    feat.id = key
    window.gj.put(key, feat, function (err, node) {
      if (err) throw err
      ready()
    })
  })

  map.on('draw:edited', function (e) {
    var fc = e.layers.toGeoJSON()
    fc.features.forEach(function (feat) {
      var key = feat.id ? feat.id : randomBytes(8).toString('hex')
      feat.id = key
      window.gj.put(key, feat, function (err, node) {
        if (err) throw err
        ready()
      })
    })
  })

  map.on('draw:deleted', function (e) {
    var fc = e.layers.toGeoJSON()
    fc.features.forEach(function (feat) {
      var key = feat.id
      window.gj.del(key, function (err, node) {
        if (err) throw err
        ready()
      })
    })
  })
}
