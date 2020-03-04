const _ = require("highland");
const { Readable } = require("stream");

// delay :: Number
//          -> (Error | NULL, *, (Error | NULL, *) -> *, () -> *)
//          -> undefined
// A highland consume function to delay each value by a specified number of ms.
// Takes a number of MS to delay
// Returns undefined
function delay (ms) {
  return (err, x, push, next) => {
    if (err) {
      push(err);
      next();
    }
    else if (x === _.nil) {
      push(null, x);
    }
    else {
      setTimeout(() => {
        push(null, x);
        next();
      }, ms);
    }
  };
}

// MockMongo :: MockMongo({
//   docs: [{ _id: Number }],
//   interval: Number - Number of ms between each doc
// })
module.exports = class MockMongo extends Readable {
  constructor (options) {
    super(Object.assign({}, options, {
      objectMode: true,
      autoDestroy: true,
    }));

    this._docs = options.docs;
    this._interval = options.interval;
    this._source = null;
  }

  startReading () {
    this._source = _(this._docs);

    this._source
      .consume(delay(this._interval))
      .each(doc => this.push(doc))
      .done(() => this.stopReading());
  }

  stopReading () {
    this.push(null);
    this._source = null;
  }

  _read (size) {
    if (!this._source) {
      this.startReading();
    }
  }
}
