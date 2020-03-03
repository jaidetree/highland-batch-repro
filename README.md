# Highland.js Batch Bug

[https://travis-ci.com/eccentric-j/highland-batch-repro]([!https://travis-ci.com/eccentric-j/highland-batch-repro.svg?branch=master](https://travis-ci.com/eccentric-j/highland-batch-repro.svg?branch=master))

# Reproduction
1. Consume mongodb doc stream in Highland
2. Batch docs in Highland stream

## Expected
- Stream should complete before timeout completes

## Actual
- Stream does not end and it times out

# Credit
- Reported by @adrian-gierakowski in https://github.com/caolan/highland/issues/693
- Repo inspired and based on https://github.com/szabolcs-szilagyi/highland-node-12

# Questions
- Is the bug an issue with Highland or the Readable implementation of MongoDB?

# Solutions
- [x] Reproduce breaking case
- [x] Try flat mapping into mongodb stream
- [ ] Create mongodb like Node Readable source
