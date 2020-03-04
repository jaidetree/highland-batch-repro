const { Readable } = require("stream");

function interval (f, ms) {
  let ref = null;
  let i = 0;

  let timer = () => {
    ref = setTimeout(() => {
      f(i += 1);
      timer();
    }, ms);
  };
  timer();

  return () => {
    if (ref) {
      clearTimeout(ref);
      ref = null;
    }
  };
}

class MockMongo extends Readable {
  constructor (options) {
    this.objectMode = true;
    this.autoDestroy = true;
    this._max = options.max;

    super(options);
  }

  _read (size) {
    this._closeSource = interval(x => {
      if (x >= this._max) {
        this.push(null);
        return;
      }

      if (!this.push(x)) {
        this._closeSource();
      }
    });
  }
}
