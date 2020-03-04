const { Readable } = require("stream");

// MockMongo :: MockMongo({
//   docs: [{ _id: Number }],
//   interval: Number - Number of ms between each doc
// })
module.exports = class MockMongo extends Readable {
  constructor (options) {
    super(Object.assign({}, options, {
      objectMode: true,
      autoDestroy: true,
      _docs: options.docs,
    }));

    this._docs = options.docs;
    this._interval = options.interval;
    this._timer = null;
  }

  // timeout :: Number -> *
  // Loops a timeout to push docs at a given delay interval.
  // Takes a seed number and emits docs at given index.
  // Returns nothing
  loopTimeout (docs) {
    if (docs.length === 0) {
      this.push(null);
      return;
    }
    this._timer = setTimeout(() => {
      this.push(docs[0]);
      this.loopTimeout(docs.slice(1));
    }, this._interval);
  }

  _read (size) {
    if (!this._timer) {
      setTimeout(() => {
        this.loopTimeout(this._docs);
      }, 3000);
    }
  }
}
