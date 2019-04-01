'use strict'
var Cursor = require('pg-cursor')
var Readable = require('stream').Readable

class PgQueryStream extends Readable {
  constructor (text, values, options) {
    var batchSize = (options || {}).batchSize || 100
    super(Object.assign({ objectMode: true, highWaterMark: batchSize }, options))
    this.cursor = new Cursor(text, values, options)
    this._reading = false
    this._closed = false
    this._buffer = []
    this.batchSize = batchSize

    // delegate Submittable callbacks to cursor
    this.handleRowDescription = this.cursor.handleRowDescription.bind(this.cursor)
    this.handleDataRow = this.cursor.handleDataRow.bind(this.cursor)
    this.handlePortalSuspended = this.cursor.handlePortalSuspended.bind(this.cursor)
    this.handleCommandComplete = this.cursor.handleCommandComplete.bind(this.cursor)
    this.handleReadyForQuery = this.cursor.handleReadyForQuery.bind(this.cursor)
    this.handleError = this.cursor.handleError.bind(this.cursor)
  }

  submit (connection) {
    this.cursor.submit(connection)
  }

  close (callback) {
    this._closed = true
    const cb = callback || (() => this.emit('close'))
    this.cursor.close(cb)
  }

  _read (size) {
    if (this._reading || this._closed) {
      return false
    }
    this._reading = true
    var readAmount = Math.max(size, this.batchSize)
    var object

    while ((object = this._buffer.shift()) && readAmount) {
      readAmount--
      if (!this.push(object)) {
        this._reading = false
        return
      }
    }

    if (!readAmount) {
      this._reading = false
      return
    }

    this.cursor.read(readAmount, (err, rows) => {
      if (this._closed) {
        return
      }
      if (err) {
        return this.emit('error', err)
      }
      // if we get a 0 length array we've read to the end of the cursor
      if (!rows.length) {
        this._closed = true
        return this.push(null)
      }

      // push each row into the stream
      this._reading = false
      while ((object = rows.shift())) {
        if (!this.push(object)) {
          this._buffer = this._buffer.concat(rows)
          return
        }
      }
    })
  }
}

module.exports = PgQueryStream
