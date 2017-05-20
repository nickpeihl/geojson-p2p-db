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

/**
 * Create a new geojson-p2p database
 * @param {Object} opts
 * @param {string} opts.log a hyperlog with a valueEncoding of `json`
 * @param {string} opts.db a levelup instace to store index data
 * @param {string} opts.store an abstract-chunk-store instance
 * @param {string} [opts.kv='kv'] an optional hyperkv instance,
 * if not specified, one will be created
 * @example
 * var hyperlog = require('hyperlog')
 * var level = require('level')
 * var fdstore = require('fd-chunk-store')
 * var gjdb = require('geojson-p2p-db')
 *
 * var gj = gjdb({
 *   log: hyperlog(level('log'), { valueEncoding: 'json' }),
 *   db: level('index'),
 *   store: fdstore(4096, '/tmp/geojson-p2p/kdb')
 * })
 */
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

/**
 * Store a new geojson feature or changeset from `doc`. `cb(err, id, node)`
 * is returned with the generated `id` and the `node` from the underlying
 * hyperlog.
 * @param {Object} value A GeoJSON Feature object containing `geometry`,
 * `properties` and `changeset` properties _or_ a changeset object with a
 * `type='changeset'` and `tags` properties. `tags.comment` is recommended for
 * storing text describing the changeset.  *Note:* The GeoJSON specification
 * allows an `id` property on Feature objects. This property will be added or
 * destructively overwritten in the geojson-p2p-db to ensure uniqueness.
 * @param {Object} [opts={}] Options to pass to hyperkv.
 * @param {Function} cb A callback function with the parameters `err`, `id`,
 * `node` where `id` is the generated id and the `node` from the underlying
 * hyperlog.
 * @example
 * gj.create({ type: 'changeset', tags: { comment: 'This is a new changeset' }},
 * function (err, id, node) {
 *   if (err) throw err
 *   var feat = {
 *     type: "Feature",
 *     properties: {
 *       beep: 'boop'
 *     },
 *     geometry: {
 *       type: 'Point',
 *       coordinates: [-123.027648, 48.695492]
 *     },
 *     changeset: id
 *   }
 *   gj.create(feat, function (err, id, node) {
 *     if (err) console.error(err)
 *     console.log('Id', id)
 *     console.log('Node', node)
 *   })
 * })
 */
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

/**
 * Replace a document `key` from `value`. The document will be created if it does
 *  not exist. `cb(err, node)` is returned with the `node` from the underlying
 * hyperlog.
 * @param {string} key Id of a document to replace with `doc`.
 * @param {Object} value A GeoJSON Feature object containing `geometry`,
 * `properties` and `changeset` properties _or_ a changeset object with a
 * `type='changeset'` and `tags` properties. `tags.comment` is recommended for
 * storing text describing the changeset.  *Note:* The GeoJSON specification
 * allows an `id` property on Feature objects. This property will be added or
 * destructively overwritten in the geojson-p2p-db to ensure uniqueness.
 * @param {Object} [opts={}] Options to pass to hyperkv.
 * @param {Function} cb A callback function with the parameters `err`, `node`
 * with the `node` from the underlying hyperlog.
 * @example
 * gj.create({ type: 'changeset', tags: { comment: 'This is a new changeset' }},
 * function (err, id, node) {
 *   if (err) throw err
 *   var feat = {
 *     type: "Feature",
 *     properties: {
 *       beep: 'boop'
 *     },
 *     geometry: {
 *       type: 'Point',
 *       coordinates: [-123.027648, 48.695492]
 *     },
 *     changeset: id
 *   }
 *   gj.create(feat, function (err, id, node) {
 *     if (err) console.error(err)
 *     console.log('Id', id)
 *     console.log('Node', node)
 *     feat.properties = {
 *       boop: 'beep'
 *     }
 *     feat.changeset = id
 *     gj.put(id, feat, function (err, node) {
 *       console.log('New node', node)
 *     })
 *   })
 * })
 */
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

/**
 * Mark the document at `key` as deleted. `cb(err, node)` is returned with the
 * `node` from the underlying hyperlog.
 * @param {string} key Id of the document to mark as deleted.
 * @param {Object} [opts={}] Options to pass to hyperkv.
 * @param {Function} cb A callback function with the parameters `err`, `node`
 * with the `node` from the underlying hyperlog.
 * @example
 * gj.create({ type: 'changeset', tags: { comment: 'This is a new changeset' }},
 *           function (err, id, node) {
 *             if (err) throw err
 *             var feat = {
 *               type: "Feature",
 *               properties: {
 *                 beep: 'boop'
 *               },
 *               geometry: {
 *                 type: 'Point',
 *                 coordinates: [-123.027648, 48.695492]
 *               },
 *               changeset: id
 *             }
 *             gj.create(feat, function (err, id, node) {
 *               if (err) console.error(err)
 *               console.log('Id', id)
 *               console.log('Node', node)
 *               gj.del(id, function (err, node) {
 *                 if (err) throw err
 *                 gj.get(id, function (err, node) {
 *                   if (err) throw err
 *                   console.log('Deleted', node)
 *                 })
 *               })
 *             })
 *           })
*/
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

/**
 * Atomically put or delete an array of documents as `rows`
 * @param {Object[]} rows Array of `row` to put or delete.
 * @param {string} rows[].type Type of transaction. Either `'put'` or `'del'`.
 * @param {string} rows[].key The id of the document to transact.
 * @param {string[]} rows[].links An array of links to ancestor ids.
 * @param {Object} rows[].value For `put`, the document to store.
 * @param {Object} [opts={}] Options to pass to hyperkv
 * @param {Function} cb A callback function with the parameters `err`, `nodes`
 * with the `nodes` from the underlying hyperlog.
 * @example
 * // With existing changeset id of 'A'
 * var rows = [
 *   { type: 'put', value: { type: 'Feature', properties: {}, geometry: { type: 'Point', coordinates: [-122.85, 48.52] }, changeset: 'A' } },
 *   { type: 'put', value: { type: 'Feature', properties: {}, geometry: { type: 'Point', coordinates: [-122.90, 48.60] }, changeset: 'A' } }
 * ]
 * gj.batch(rows, function (err, nodes) {
 * if (err) throw err
 * console.log(nodes)
 * })
 */
DB.prototype.batch = function (rows, opts, cb) {
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

/**
 * Get the documents with the id `key`.
 * @param {string} key The id of the documents to retrieve.
 * @param {Object} [opts={}] Options to pass to hyperkv.
 * @param {Function} cb A callback function with the parameters `err`, `docs`
 * where `docs` is an object mapping hyperlog hashes to current document values.
 * If a document has been deleted, it will only have the properties
 * `{ id: <id>, version: <version>, deleted: true }`.
 * @example
 * gj.create({ type: 'changeset', tags: { comment: 'This is a new changeset' } },
 *           function (err, id, node) {
 *             if (err) throw err
 *             var feat = {
 *               type: "Feature",
 *               properties: {
 *                 beep: 'boop'
 *               },
 *               geometry: {
 *                 type: 'Point',
 *                 coordinates: [-123.027648, 48.695492]
 *               },
 *               changeset: id
 *             }
 *             gj.create(feat, function (err, id, node) {
 *               if (err) console.error(err)
 *               console.log('Id', id)
 *               gj.get(id, function (err, nodes) {
 *                 if (err) throw err
 *                 console.log('Nodes', nodes)
 *               })
 *             })
 *           })
*/
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
/**
 * Query the database using latitude/longitude bounds.
 * @param {Array<Array<Number>>} q An array of `[[ minLat, maxLat], [minLng, maxLng]]`
 * coordinate pairs to specify a bounding box.
 * @param {Object} [opts={}] Options to pass to kdb-tree-store query.
 * @param {Function} cb A callback function with the parameters `err, res`
 * where `res` is an array of documents each containing an `id` property and a
 * `version` property which is the hash key from the underlying hyperlog.
 * Deleted documents will only have the properties
 * `{ id: <id>, version: <version>, deleted: true }`.
 * @example
 * // With existing changeset id of 'A'
 * var rows = [
 *   { type: 'put', value: { type: 'Feature', properties: {}, geometry: { type: 'Point', coordinates: [-122.85, 48.52] }, changeset: 'A' } },
 *   { type: 'put', value: { type: 'Feature', properties: {}, geometry: { type: 'Point', coordinates: [-122.90, 48.70] }, changeset: 'A' } }
 * ]
 *
 * gj.batch(rows, function (err, nodes) {
 *   if (err) throw err
 *   console.log('Created', nodes)
 *   gj.query([ [ 48.50, 48.60 ], [ -122.89, -122.80 ] ], function (err, res) {
 *     if (err) throw err
 *     console.log('Results', res)
 *   })
 * })
*/
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

/**
 * Return a readable object stream of query results contained in the
 * query `q`.
 * @param {Array<Array<Number>>} q An array of `[[ minLat, maxLat], [minLng, maxLng]]`
 * coordinate pairs to specify a bounding box.
 * @param {Object} [opts={}] Options to pass to kdb-tree-store query.
*/
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

/**
 * Get a list of document version ids in a changeset by the changeset id `key`
 * @param {string} key Id of changeset.
 * @param {Function} [cb] An optional callback function with the parameters
 * `err`, `versions` where `versions` is an array of changeset ids. If no
 * callback is specified, a readable object stream is returned.
 * @example
 * gj.create({ type: 'changeset', tags: { comment: 'This is a new changeset' }},
 *           function (err, changesetId, node) {
 *             if (err) throw err
 *             var rows = [
 *               { type: 'put', value: { type: 'Feature', properties: {}, geometry: { type: 'Point', coordinates: [-122.85, 48.52] }, changeset: changesetId } },
 *               { type: 'put', value: { type: 'Feature', properties: {}, geometry: { type: 'Point', coordinates: [-122.90, 48.60] }, changeset: changesetId } }
 *             ]
 *             gj.batch(rows, function (err, nodes) {
 *               if (err) throw err
 *               gj.getChanges(changesetId, function (err, nodes) {
 *                 if (err) throw err
 *                 console.log(nodes)
 *               })
 *             })
*/
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
