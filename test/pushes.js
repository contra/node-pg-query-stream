var helper = require('./helper')
var QueryStream = require('../')
var Writable = require('stream').Writable
var assert = require('assert')

helper('pushes', function (client) {
  it('stops pushing data when push() returns false and resumes on _read()', function (done) {
    var readable = client.query(new QueryStream('SELECT * FROM generate_series(0, $1)', [500]))
    var writable = new Writable({ highWaterMark: 100, objectMode: true })
    var shouldPushAgain = true

    readable.original_read = readable._read
    readable._read = function (size) {
      shouldPushAgain = true
      return this.original_read(size)
    }

    readable.originalPush = readable.push
    readable.push = function (data) {
      if (!shouldPushAgain && !(shouldPushAgain = this.originalPush(data))) {
        assert(false)
      } else {
        return (shouldPushAgain = this.originalPush(data))
      }
    }

    writable._write = function (chunk, encoding, callback) {
      setImmediate(callback)
    }

    readable.pipe(writable).on('finish', done)
  })
})
