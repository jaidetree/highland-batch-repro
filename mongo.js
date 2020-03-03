const assert = require("assert");
const _ = require("highland");
const MongoClient = require('mongodb').MongoClient;

// range :: (Number, Number) -> [Number]
// Creates an array from start number up to end number
function range (start, end) {
  let xs = [];

  for (let i = start; i < end; i+=1) {
    xs.push(i);
  }

  return xs;
}

// merge :: (...HighlandStream<*>) -> HighlandStream<*> -> HighlandStream<*>
// Merge new streams into source stream
// Takes any number of highland stream args
// Returns a function that merges new streams into source stream then returns
// the merged highland stream
function merge (...streams) {
  return source => _.merge(
    [source].concat(streams),
  );
}

// timeout :: Number -> HighlandStream<Number>
// Create a timeout stream to emit Date.now() in x ms
// Takes the number of ms to weait before emititng Date.now()
// Returns a Highland Stream that will emit Date.now() then close
function timeout (ms) {
  return _(push => {
    setTimeout(() => {
      push(null, Date.now());
      push(null, _.nil);
    }, ms);
  });
}

// logStream :: (Error | NULL, *, (Error | NULL, *) -> *, () -> *) -> *
// Describes the mechanics of the stream, when it's pushing, when its closed,
// and when an error occurs. Function should be used with highland's .consume.
// Takes an Error instance or null, the incoming value, a function to push
// downstream, and a function to continue to next value.
function logStream (err, x, push, next) {
  if (err) {
    console.error("logMechanics: Received error", x);
    push(err);
    next();
  }
  else if (x === _.nil) {
    console.log("logMechanics: Upstream closed");
    push(null, x);
  }
  else {
    console.log("logMechanics: Pushing value", x);
    push(null, x);
    next();
  }
}

// fetchDocs :: MongoCollection -> ReadableStream<Doc>
// Takes a MongoCollection of test docs
// Returns a stream of batched doc results
function fetchDocs (col) {
  const cursor = col.find();

  cursor.batchSize(2);
  const source = cursor.stream();

  // Make sure our mongodb stream is ending
  source.on("end", () => {
    console.log("END");
  });
  source.on("close", () => {
    console.log("CLOSE");
  });

  return _(source);
}


// append :: ([xs], x) -> [xs... y]
// Appends a value to the end of an array.
// Takes an array and a value to append to it
// Returns a new array with the new value appended
function append (xs, x) {
  return xs.concat(x);
}

// Create a sample of 22 docs
const docs = range(0, 22)
      .map(_id => ({ _id }));

// Create our mongodb connection opts
const url = `mongodb://${process.env['MONGODB_URL']}:27017`;
const opts = {
  useUnifiedTopology: true,
};


async function main () {
  const client = await MongoClient.connect(url, opts);
  const db = client.db("test");
  const col = db.collection("test");
  await col.removeMany();
  await col.insertMany(docs);

  // This function will explode in six seconds
  timeout(6000)
    .done(() => {
      throw new Error("APPLICATION TIMED OUT");
      client.close();
      process.exit(1);
    });

  return _.of(col)
    .flatMap(fetchDocs)
    .batch(11)
    .consume(logStream)
    .flatMap(_)
    .reduce(append, [])
    .toCallback((err, xs) => {
      client.close();

      if (err) {
        console.error(err);
        process.exit(1);
      }

      assert.deepEqual(xs, docs, `DOCS WERE NOT RETUREND. INSTEAD FOUND "${xs}"`);

      console.log("Completed!", xs);
      process.exit(0);
    });
}

// Run our async main function
main();
