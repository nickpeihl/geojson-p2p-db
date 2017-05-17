var hyperkv = require('hyperkv')
var hyperkdb = require('hyperlog-kdb-index')
var kdbtree = require('kdb-tree-store')
var sub = require('subleveldown')
var randomBytes = require('randombytes')
var once = require('once')
var through = require('through2')
var to = require('to2')
var readonly = require('read-only-stream')
var xtend = require('xtend')
var join = require('hyperlog-join')
var inherits = require('inherits')
var EventEmitter = require('events').EventEmitter
var lock = require('mutexify')
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

  self.refs = function () {
    self.emit('error', 'Not implemented')
  }

  self.changeset = join({
    log: self.log,
    db: sub(self.db, 'c'),
    map: function (row, cb) {
      if (!row.value) cb()
      var v = row.value.v
      if (v && v.changeset) {
        cb(null, { type: 'put', key: v.changeset, value: 0 })
      } else cb()
    }
  })
  self.changeset.on('error', function (err) { self.emit('error', err) })
}

DB.prototype._getReferers = function (version, cb) {
  cb()
}

DB.prototype.ready = function (cb) {
  cb = once(cb || noop)
  var pending = 2
  this.kdb.ready(ready)
  this.changeset.dex.ready(ready)
  function ready () { if (--pending <= 0) cb() }
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
  var feat = xtend(value, { id: key })
  self.put(key, feat, opts, function (err, node) {
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

  if (!opts || !opts.links) {
    self.kv.get(id, function (err, docs) {
      if (err) cb(err)

      docs = mapObj(docs, function (version, doc) {
        if (doc.deleted) {
          return {
            id: id,
            version: version,
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
             version: node.key,
             deleted: true
           }
           : xtend(node.value.v, {
             id: node.value.k,
             version: node.key
           }))
    })
  }

  function handleLinks (docs) {
    var fields = {}
    var links = Object.keys(docs)
    links.forEach(function (ln) {
      var v = docs[ln]
      if (!fields.points) fields.points = []
      fields.points = fields.points.concat(geojsonCoords(v))
    })
    cb(null, [ { type: 'del', key: id, links: links, fields: fields } ])
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
          id: key,
          version: version,
          deleted: true
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
    var seen = {}
    for (var i = 0; i < pts.length; i++) {
      var pt = pts[i]
      pending++
      self._collectFeatures(kdbPointToVersion(pt), seen, function (err, r) {
        if (err) cb(err)
        if (r) res = res.concat(r)
        if (--pending <= 0) done()
      })
    }
    if (--pending <= 0) done()
  }
}

DB.prototype._collectFeatures = function (version, seenAccum, cb) {
  cb = once(cb || noop)
  var self = this
  if (seenAccum[version]) cb(null)
  var res = []
  var added = {}
  var pending = 1
  var selfNode
  self.log.get(version, function (err, node) {
    if (err) cb(err)
    else {
      selfNode = node
    }
    self._getReferers(version, onLinks)
  })

  function onLinks (err, links) {
    // TODO Handle error
    if (!links) links = []

    if (links.length === 0 && selfNode && !seenAccum[selfNode.key]) {
      addDocFromNode(selfNode)
      seenAccum[selfNode.key] = true
    }
    selfNode = null

    links.forEach(function (link) {
      if (seenAccum[link]) return
      seenAccum[link] = true
      pending++
      self.log.get(link, function (err, node) {
        if (err) self.emit('error', err)
        if (!err) {
          addDocFromNode(node)

          if (node && node.value && node.value.k && node.value.v) {
            pending++
            self.get(node.value.k, function (err, docs) {
              if (err) cb(err)
              Object.keys(docs).forEach(function (key) {
                addDoc(node.value.k, key, docs[key])
              })
              if (--pending <= 0) cb(null, res)
            })
          }
        }
        if (--pending <= 0) cb(null, res)
      })
      pending++
    })
    if (--pending <= 0) cb(null, res)
  }

  function addDocFromNode (node) {
    if (node && node.value && node.value.k && node.value.v) {
      return addDoc(node.value.k, node.key, node.value.v)
    } else if (node && node.value && node.value.d) {
      return addDoc(node.value.d, node.key, { deleted: true })
    }
  }

  function addDoc (id, version, doc) {
    if (!added[version]) {
      doc = xtend(doc, {
        id: id,
        version: version
      })
      res.push(doc)
      added[version] = true
    }

    return doc
  }
}

DB.prototype.queryStream = function (q, opts) {
  var self = this
  if (!opts) opts = {}
  var stream = through.obj(write)
  var seen = {}

  self.ready(function () {
    var r = self.kdb.queryStream(q, opts)
    r.on('error', stream.emit.bind(stream, 'error'))
    r.pipe(stream)
  })
  return readonly(stream)

  function write (row, enc, next) {
    next = once(next)
    var tr = this
    self._collectFeatures(kdbPointToVersion(row), seen, function (err, res) {
      if (err) next()
      if (res) tr.push(res)
      next()
    })
  }
}

DB.prototype.getChanges = function (key, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  var r = this.changeset.list(key, opts)
  var stream = r.pipe(through.obj(write))
  if (cb) collectObj(stream, cb)
  return readonly(stream)

  function write (row, enc, next) {
    this.push(row.key)
    next()
  }
}

function mapKdb (row, next) {
  if (!row.value) return null
  var v = row.value.v
  var d = row.value.d
  if (v && v.type === 'Feature' && v.geometry && v.geometry.coordinates) {
    next(null, { type: 'put', points: geojsonCoords(v).map(ptf) })
  } else if (d && Array.isArray(row.value.points)) {
    var pts = row.value.points.map(ptf)
    next(null, { type: 'put', points: pts })
  } else next()
}

function noop () {}

function collectObj (stream, cb) {
  cb = once(cb)
  var rows = []
  stream.on('error', cb)
  stream.pipe(to.obj(write, end))
  function write (x, enc, next) {
    rows.push(x)
    next()
  }
  function end () { cb(null, rows) }
}

function mapObj (obj, fn) {
  Object.keys(obj).forEach(function (key) {
    obj[key] = fn(key, obj[key])
  })
  return obj
}

function kdbPointToVersion (pt) {
  return pt.value.toString('hex')
}

function ptf (v) {
  return [v[1], v[0]]
}
