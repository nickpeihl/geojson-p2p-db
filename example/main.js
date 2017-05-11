var hyperlog = require('hyperlog')
var level = require('level-browserify')
var idbstore = require('idb-chunk-store')
var dragDrop = require('drag-drop')
var geojsonhint = require('@mapbox/geojsonhint')

var db = {
  log: level('log'),
  index: level('index')
}

var gjdb = require('../')
window.gj = gjdb({
  log: hyperlog(db.log, { valueEncoding: 'json' }),
  db: db.index,
  store: idbstore(4096)
})

var el = document.createElement('div')
el.setAttribute('id', 'dropTarget')
el.setAttribute('style', 'width:100vw; height:100vh;')
el.innerText = 'Drag and Drop a GeoJSON file here to store in the database'
document.body.appendChild(el)

dragDrop('#dropTarget', function (files) {
  files.forEach(function (file) {
    var reader = new window.FileReader()
    reader.addEventListener('load', function (e) {
      var arr = new Uint8Array(e.target.result)
      var buf = new Buffer(arr)
      geojsonhint.hint(buf.toString())
      var data = JSON.parse(buf.toString())
      if (data.features && Array.isArray(data.features)) {
        data.features.map(function (f) {
          window.gj.create(f)
        })
      } else {
        window.gj.create(data)
      }
    })
    reader.readAsArrayBuffer(file)
  })
})
