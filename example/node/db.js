var hyperlog = require('hyperlog')

var level = require('level-browserify')
var db = {
  log: level('/tmp/geojson-p2p/log'),
  index: level('/tmp/geojson-p2p/index')
}
var fdstore = require('fd-chunk-store')
var storefile = '/tmp/geojson-p2p/kdb'

var gjdb = require('../../')
var gj = gjdb({
  log: hyperlog(db.log, { valueEncoding: 'json' }),
  db: db.index,
  store: fdstore(4096, storefile)
})

if (process.argv[2] === 'create') {
  var value = JSON.parse(process.argv[3])
  gj.create(value, function (err, key, node) {
    if (err) console.error(err)
    else console.log(key)
  })
} else if (process.argv[2] === 'query') {
  var q = process.argv.slice(3).map(csplit)
  gj.query(q, function (err, pts) {
    if (err) console.error(err)
    else pts.forEach(function (pt) {
      console.log(JSON.stringify(pt, null, 2))
    })
  })
}

function csplit (x) { return x.split(',').map(Number) }
