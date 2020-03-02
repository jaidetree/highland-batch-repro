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

// prepareDB :: [{ _id: Number }] -> MongoClient -> {
//    col: MongoCollection,
//    client: MongoClient,
// }
// Takes a list of docs
// Returns a function that takes a mongodb connection and...
// - selects the test db
// - selects the test collection
// - Removes any docs already in the collection
// - Adds new docs into the collection
// - Emits the collection
function prepareDB (docs) {
  return client => {
    const db = client.db("test");
    const col = db.collection("test");
    const actions = [
      () => col.removeMany(),
      () => col.insertMany(docs),
    ];

    // Teardown old data, insert new data, return state for queries
    return _(actions)
     .map(action => _(action()))
     .mergeWithLimit(1)
     .last()
     .map(() => ({ col, client }));
  };
}

// fetchDocs :: MongoCollection -> ReadableStream<Doc>
// Takes a MongoCollection of test docs
// Returns a stream of batched doc results
function fetchDocs ({ client, col }) {
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

  return _(source)
    .batch(11)
    .consume(logMechanics)
    .flatMap(_)
    .reduce(append, [])
    .map(docs => ({ docs, client }));
}

function logMechanics (err, x, push, next) {
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


// append :: ([xs], x) -> [xs... y]
// Appends a value to the end of an array.
// Takes an array and a value to append to it
// Returns a new array with the new value appended
function append (xs, x) {
  return xs.concat(x);
}

const docs = range(0, 22)
      .map(_id => ({ _id }));

const opts = {
  useUnifiedTopology: true,
};



_(MongoClient.connect('mongodb://root:example@mongo:27017', opts))
 .flatMap(prepareDB(docs))
 .flatMap(fetchDocs)
 .toCallback((err, results) => {
   if (err) {
     console.error(err);
     process.exit(1);
   }

   assert.deepEqual(results.docs, docs, `DOCS WERE NOT RETUREND. INSTEAD FOUND "${xs}"`);

   console.log("Completed!", results.docs);
   results.client.close();
   process.exit(0);
 });

timeout(6000)
  .done(() => {
    throw new Error("APPLICATION TIMED OUT");
    process.exit(1);
  });
