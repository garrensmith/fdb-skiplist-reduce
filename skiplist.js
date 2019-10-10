const assert = require('assert');
const fdb = require('foundationdb');
const ks = require('foundationdb').keySelector;
fdb.setAPIVersion(600); // Must be called before database is opened

const PREFIX = 'skiplist';

const db = fdb.openSync() // or openSync('/path/to/fdb.cluster')
  .at(PREFIX) // database prefix for all operations
  .withKeyEncoding(fdb.encoders.tuple)
  .withValueEncoding(fdb.encoders.json); // automatically encode & decode values using JSON
 
const MAX_LEVELS = 6;
const LEVEL_FAN_POW = 1; // 2^X per level or (1 / 2^X) less than previous level
const END = 0xFF;

// const keys = [
//     [2017, 03, 1],

//     [2017, 04, 1],
//     [2017, 04, 15],
//     [2017, 05, 1],
//     [2018, 03, 1],
//     [2018, 04, 1],
//     [2018, 05, 1],
//     [2019, 03, 1],
//     [2019, 04, 1],
//     [2019, 05, 1]
// ].map(k => JSON.stringify(k));

const kvs = [
    ['[2017,3,1]', 9],
    ['[2017,4,15]', 6],
    ['[2017,4,1]', 7],
    ['[2017,5,1]', 9],
    ['[2018,3,1]', 6],
    ['[2018,4,1]', 7],
    ['[2018,5,1]', 7],
    ['[2019,3,1]', 4],
    ['[2019,4,1]', 6],
    ['[2019,5,1]', 7]
  ]

// UTILS

const getRandom = (min, max) => {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min)) + min; //The maximum is exclusive and the minimum is inclusive
  }

function hashCode(s) {
    for(var i = 0, h = 0; i < s.length; i++)
        h = Math.imul(31, h) + s.charCodeAt(i) | 0;
    return h;
}

const hashCalc = (key, level, pow) => {
    const keyHash = hashCode(key.toString());
    const out = (keyHash & ((1 << (level * pow)) - 1));
    if (out !== 0) {
        return false;
    }

    return true;
}

// _sum but pretend its more complex
const rereduce = (values) => {
    const out = values.reduce((acc, val) => {
        return acc + val;
    }, 0);

    // console.log('rereduce', values, out);
    return out;
};

// OPERATIONS

const clear = async () => {
    await db.doTransaction(async tn => {
        tn.clearRangeStartsWith([]);
    });
}

const create = async () => {
    await db.doTransaction(async tn => {
        for(let level = 0; level <= MAX_LEVELS; level++) {
            await insertAtLevel(tn, '0', 0, level);
        }
    });

    console.log('setup done');
    for ([key, val] of kvs) {
        await insert(key, val);
    }
};

const insert = async (key, value) => {
    return db.doTransaction(async tn => {
        for(let level = 0; level <= MAX_LEVELS; level++) {
            if (level === 0) {
                insertAtLevel(tn, key, value, 0);
                continue;
            }
            const previous = await getPrevious(tn, key, level);
            if (hashCalc(key, level, LEVEL_FAN_POW)) {
                const lowerLevel = level - 1;
                // update previous node
                const newPrevRange = await getRange(tn, previous.key, key, lowerLevel);
                const prevValues = newPrevRange.map(kv => kv.value);
                const newPrevValue = rereduce(prevValues)
                await insertAtLevel(tn, previous.key, newPrevValue, level);

                // calculate new nodes values
                const next = await getNext(tn, previous.key, level);
                const newRange = await getRange(tn, key, next.key, lowerLevel);
                const newValues = newRange.map(kv => kv.value);
                const newValue = rereduce(newValues)
                await insertAtLevel(tn, key, newValue, level);
            } else {
                const newValue = rereduce([previous.value, value]);
                await insertAtLevel(tn, previous.key, newValue, level);
            }
        }
    })
};

const insertAtLevel = async (tn, key, value, level) => {
    return await tn.set([level, key], value);
};

const getRange = async (tn, start, end, level) => {
    const kvs = await tn.getRangeAll([level, start], [level, end]);

    return kvs.map(([key, value]) => {
        return {
            key,
            value
        };
    });
};

const getRangeInclusive = async (tn, start, end, level) => {
    const kvs = await tn.getRangeAll(
        [level, start], 
        ks.firstGreaterThan([level, end])
        );

    return kvs.map(([key, value]) => {
        return {
            key,
            value
        };
    });
}

const getKV = (item) => {
    const [key, value] = item.value;
    return {
        key: key[1],
        value: value
    };
}

const getNext = async (tn, key, level) => {
    const iter = await tn.snapshot().getRange(
        ks.firstGreaterThan([level, key]),
        [level, END],
        {limit: 1}
    )

    const item = await iter.next();
    if (item.done) {
        return {
            key: [level, END],
            value: 0
        };
    }

    return getKV(item);
};

const getPrevious = async (tn, key, level) => {
    const iter = await tn.snapshot().getRange(
        ks.lastLessThan([level, key]),
        ks.firstGreaterOrEqual([level, key]),
        {limit: 1}
    )

    //TODO: add a conflict key
    const item = await iter.next();
    return getKV(item);
};

const getKeyOrNearest = async (tn, key, level, endkey) => {
    const _endkey = endkey ? endkey : END;
    const iter = await tn.snapshot().getRange(
        ks.firstGreaterOrEqual([level, key]),
        ks.lastLessThan([level, _endkey]),
        {limit: 1}
    )
    
    //TODO: add a conflict key
    const item = await iter.next();
    if (item.done) {
        // return {
        //     key: null,
        //     value: 0
        // };
        return null;
    }

    return getKV(item);
};

const getKeyOrFirstBefore = async (tn, key, level) => {
    const iter = await tn.snapshot().getRange(
        [level, '0'],
        [level, key],
        {limit: 1, reverse: true}
    )
    
    //TODO: add a conflict key
    const item = await iter.next();
    if (item.done) {
        // return {
        //     key: null,
        //     value: 0
        // };
        return null;
    }

    return getKV(item);
};

const print = async () => {
    let total = 0;
    await db.doTransaction(async tn => {
        for(let level = 0; level <= MAX_LEVELS; level++) {
            let levelTotal = 0;
            const levelResults = await tn.getRangeAll([level, "0"], [level, END]);
            const keys = levelResults.map(([[_, key], val]) => {
                const a = {};
                a[key] = val;
                if (level === 0) {
                    total += val;
                }

                levelTotal += val;
                // return [key, val];
                return a;
            });

            console.log(`Level ${level}`, keys);
            // console.log(`Total ${total}, Level ${levelTotal} are equal`, total == levelTotal);
            assert.equal(levelTotal, total, `Level ${level} values not equal`);
        }
    });

    return {
        total
    };
};

const traverse = async (tn, level, current, endkey, acc) => {
    console.log('END', endkey);
    const nearestLevelUp = await getKeyOrNearest(tn, current.key, level + 1, endkey.key);
    console.log('levelup', level, current, nearestLevelUp);
    if (nearestLevelUp !== null && nearestLevelUp.key === current.key) {
        // go another level up
        return await traverse(tn, level + 1, current, endkey, acc);
    } else {
        const rangeEndKey = nearestLevelUp === null ? endkey.key : nearestLevelUp.key;
        const results = await getRangeInclusive(tn, current.key, rangeEndKey, level);
        // const results = await getRange(tn, current.key, rangeEndKey, level);
        const vals = results.map(r => r.value);
        const reduceVals = rereduce(vals);
        acc.push(reduceVals);

        if (nearestLevelUp === null || nearestLevelUp.key === endkey.key) {
            return rereduce(acc);
        }

        return traverse(tn, level + 1, nearestLevelUp, endkey, acc);
    }
}

const query = async (opts) => {
    return await db.doTransaction(async tn => {
        let endkey = {key: END, value: 0};
        if (opts.group_level === 0 && !opts.startkey) {
                const results = await getRange(tn, '0', END, MAX_LEVELS);
                if (results.length > 1) {
                    const vals = results.map(kv => kv.value);
                    const total = rereduce(vals);
                    return {
                        key: null,
                        value: total
                    };
                }

                return {
                    key: null,
                    value: results[0].value
                }
        }

        if (opts.endkey) {
            endkey = await getKeyOrFirstBefore(tn, JSON.stringify(opts.endkey), 0);
            console.log('2019,03,1,end', opts.endkey, endkey);
        }

        if (opts.group_level === 0 && opts.startkey) {
            const startkey = await getKeyOrNearest(tn, JSON.stringify(opts.startkey), 0);
            const results = await traverse(tn, 0, startkey, endkey, []);
            console.log('final results', results);
            return {
                key: null,
                value: results
            };
        }
    });
};

const run = async () => {
    await clear();
    await create();
    await print();

    // const result = await query({group_level: 0});
    // assert(info.total, result.value);
    // console.log('GROUP_LEVEL = 0', result);

    // const result2 = await query({group_level:0, startkey: [2018, 03, 2]});
    // assert.deepEqual(result2, {
    //     key: null,
    //     value: 31
    // });

    const result3 = await query({
        group_level: 0,
        startkey: [2018, 03, 2],
        endkey: [2019, 03, 2],

    })

    assert.deepEqual(result3, {
        key: null,
        value: 18
    });
};

run();