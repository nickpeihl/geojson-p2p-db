var hyperkv = require('hyperkv')
var hyperkdb = require('hyperlog-kdb-index')
var kdbtree = require('kdb-tree-store')
var sub = require('subleveldown')
var EventEmitter = require('events').EventEmitter
var inherits = require('inherits')
var lock = require('mutexify')
var once = require('once')
var randomBytes = require('randombytes')
var xtend = require('xtend')
var uniq = require('uniq')
var mapLimit = require('map-limit')
var geojsonCoords = require('@mapbox/geojson-coords')

module.exports = DB

inherits(DB, EventEmitter)

function DB (opts) {
  var self = this
  if (!(self instanceof DB)) return new DB(opts)

  self.log = opts.log
  self.db = opts.db

  self.kv = opts.kv
    ? opts.kv
    : hyperkv({
      log: self.log,
      db: sub(self.db, 'kv')
    })
  self.kv.on('error', function (err) {
    self.emit('error', err)
  })

  self.lock = lock()

  self.kdb = hyperkdb({
    log: self.log,
    store: opts.store,
    db: sub(self.db, 'kdb'),
    kdbtree: kdbtree,
    types: [ 'float64', 'float64' ],
    map: mapKdb
  })
  self.kdb.on('error', function (err) {
    self.emit('error', err)
  })
}

DB.prototype.ready = function (cb) {
  cb = once(cb || noop)
  this.kdb.ready(cb)
}

DB.prototype.create = function (value, opts, cb) {
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  if (!cb) cb = noop
  var key = randomBytes(8).toString('hex')
  self.put(key, value, opts, function (err, node) {
    cb(err, key, node)
  })
}

DB.prototype.put = function (key, value, opts, cb) {
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  if (!cb) cb = noop
  self.lock(function (release) {
    self.kv.put(key, value, opts, function (err, node) {
      release(cb, err, node)
    })
  })
}

DB.prototype.del = function (key, opts, cb) {
  // cb(new Error('Not implemented yet'))
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  cb = once(cb || noop)

  var rows = [{
    type: 'del',
    key: key,
    links: opts.links
  }]

  self.batch(rows, opts, function (err, nodes) {
    if (err) cb(err)
    else cb(null, nodes[0])
  })
}

DB.prototype._getDocumentDeletionBatchOps = function (id, opts, cb) {
  var self = this

  if (!opts || !opts.links)  {
    self.kv.get(id, function (err, docs) {
      if (err) cb(err)

      docs = mapObj(docs, function (version, doc) {
        if (doc.deleted) {
          return {
            id: id,
            deleted: true
          }
        } else {
          return doc.value
        }
      })

      handleLinks(docs)
    })
  } else {
    // Fetch all versions of documents that match `opts.links`
    mapLimit(opts.links, 10, linkToDocument, function (err, docList) {
      if (err) cb(err)
      var docs = {}
      docList.forEach(function (doc) {
        docs[doc.version] = doc
      })
      handleLinks(docs)
    })
  }

  function linkToDocument (link, done) {
    self.log.get(link, function (err, node) {
      if (err) done(err)

      done(null, node.value.d
           ? {
             id: node.value.d,
             deleted: true
           }
           : xtend(node.value.v, {
             id: node.value.k
           }))
    })
  }

  function handleLinks (docs) {
    var links = Object.keys(docs)
    cb(null, [ { type: 'del', key: id, links: links } ])
  }
}

DB.prototype.batch = function (rows, opts, cb) {
  // cb(new Error('Not implemented yet'))
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  cb = once(cb || noop)

  var batch = []
  self.lock(function (release) {
    var done = once(function () {
      self.kv.batch(batch, opts, function (err, nodes) {
        release(cb, err, nodes)
      })
    })

    var pending = 1 + rows.length
    rows.forEach(function (row) {
      var key = row.key
          ? row.key
          : row.id
      if (!key) {
        key = row.key = randomBytes(8).toString('hex')
      }

      if (row.links && !Array.isArray(row.links)) {
        return cb(new Error('row has a "links" field that isn\'t an array'))
      } else if (!row.links && row.links !== undefined) {
        return cb(new Error('row has a "links" field that is non-truthy but not undefined'))
      }

      if (row.type === 'put') {
        batch.push(row)
        if (--pending <= 0) done()
      } else if (row.type === 'del') {
        var xrow = xtend(opts, row)
        self._getDocumentDeletionBatchOps(key, xrow, function (err, xrows) {
          if (err) release(cb, err)
          batch.push.apply(batch, xrows)
          if (--pending <= 0) done()
        })
      } else {
        var err = new Error('unexpected row type: ' + row.type)
        process.nextTick(function () { release(cb, err)})
      }
    })
    if (--pending <= 0) done()
  })
}

DB.prototype.get = function (key, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  this.kv.get(key, function (err, docs) {
    if (err) return cb(err)
    docs = mapObj(docs, function (version, doc) {
      if (doc.deleted) {
        return {
          type: 'Feature',
          id: key,
          geometry: null,
          properties: null
        }
      } else {
        return xtend(doc.value, { id: key })
      }
    })
    cb(null, docs)
  })
}

DB.prototype.query = function (q, opts, cb) {
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  cb = once(cb || noop)
  var res = []

  var done = once(function () {
    cb(null, res)
  })

  self.ready(function () {
    self.kdb.query(q, opts, onquery)
  })

  function onquery (err, pts) {
    if (err) cb(err)
    var pending = 1
    var keys = uniq(pts.map(kdbPointToVersion))
    for (var i = 0; i < keys.length; i++) {
      var key = keys[i]
      pending++
      self.log.get(key, function (err, node) {
        if (err) cb(err)
        addDocFromNode(node)
        if (--pending <= 0) done()
      })
    }
    if (--pending <= 0) done()
  }

  function addDocFromNode (node) {
    if (node && node.value && node.value.k && node.value.v) {
      addDoc(node.value.k, node.key, node.value.v)
    } else if (node && node.value && node.value.d) {
      addDoc(node.value.d, node.key, { deleted: true })
    }
  }

  function addDoc (id, version, doc) {
    doc = xtend(doc, {
      id: id
    })
    res.push(doc)
  }
}

DB.prototype._collectFeatures = function (version, seenAccum, cb) {
  cb = once(cb || noop)
  var self = this
  if (seenAccum[version]) cb(null)
  self.log.get(version, function (err, doc) {
    if (err) cb(err)
    seenAccum[doc.key] = true
    cb(null, doc)
  })
}

function mapKdb (row, next) {
  if (!row.value) return null
  var v = row.value.v
  if (v && v.type) {
    next(null, { type: 'put', points: geojsonCoords(v) })
  } else next()
}

function noop () {}

function mapObj (obj, fn) {
  Object.keys(obj).forEach(function (key) {
    obj[key] = fn(key, obj[key])
  })
  return obj
}

function kdbPointToVersion (pt) {
  return pt.value.toString('hex')
}
