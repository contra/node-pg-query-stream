var concat = require('concat-stream')
var tester = require('stream-tester')
var JSONStream = require('JSONStream')
var assert = require('assert')

var QueryStream = require('../')

require('./helper')('pauses', function (client) {
  it('pauses', function (done) {
    this.timeout(5000)
    var stream = new QueryStream('SELECT * FROM generate_series(0, $1) num', [200], { batchSize: 2, highWaterMark: 2 })
    var query = client.query(stream)
    var pauser = tester.createPauseStream(0.1, 100)
    query
      .pipe(JSONStream.stringify())
      .pipe(pauser)
      .pipe(
        concat(function (json) {
          JSON.parse(json)
          done()
        })
      )
  })

  it('keeps a stable internal buffer size when paused/resumed', function (done) {
    this.timeout(5000)

    var stream = client.query(new QueryStream('SELECT * FROM generate_series(0, $1)', [10000], { batchSize: 100 }))
    var results = []
    var concurrency = 50

    stream.on('data', function (result) {
      results.push(result)

      if (results.length === concurrency) {
        stream.pause()

        setTimeout(function () {
          results = []
          stream.resume()
        }, 10)
      }

      assert(stream._readableState.buffer.length <= stream.batchSize)
    })

    stream.on('end', done)
  })
})
