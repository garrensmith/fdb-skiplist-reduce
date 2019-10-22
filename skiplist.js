const assert = require('assert');
const util = require('util');
const fdb = require('foundationdb');
const ks = require('foundationdb').keySelector;
fdb.setAPIVersion(600); // Must be called before database is opened

const PREFIX = 'skiplist';

const db = fdb.openSync() // or openSync('/path/to/fdb.cluster')
  .at(PREFIX) // database prefix for all operations
  .withKeyEncoding(fdb.encoders.tuple)
  .withValueEncoding(fdb.encoders.json); // automatically encode & decode values using JSON
 
const MAX_LEVELS = 6;
const LEVEL_FAN_POW = 2; // 2^X per level or (1 / 2^X) less than previous level
const END = 0xFF;

let stats;

const SHOULD_LOG = false;
const log = (...args) => {
    if (!SHOULD_LOG) {
        return;
    }

    console.log(...args);
}

const resetStats = () => {
    stats = {
        "0": [],
        "1": [],
        "2": [],
        "3": [],
        "4": [],
        "5": [],
        "6": [],
    };
}

const kvs = [
    [[2017,3,1], 9],
    [[2017,4,1], 7], 
    [[2019,3,1], 4], // out of order check
    [[2017,4,15], 6],
    [[2018,4,1], 3],  
    [[2017,5,1], 9],
    [[2018,3,1], 6],
    [[2018,4,1], 4], // duplicate check
    [[2018,5,1], 7],
    [[2019,4,1], 6],
    [[2019,5,1], 7]
  ];

// UTILS

const getRandom = (min, max) => {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min)) + min; //The maximum is exclusive and the minimum is inclusive
  }

const getRandomKey = (min, max) => {
    return [getRandom(min, max), getRandom(1, 12), getRandom(1, 30)];
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

    // log('rereduce', values, out);
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

    log('setup done');
    for ([key, val] of kvs) {
        await db.doTransaction(async tn => {
            await insert(tn, key, val);
        });
    }
};

const rawKeys = []
const createLots = async () => {
    const docsPerTx = 1000;
    console.time('total insert');
    for (let i = 0; i <= 100000; i+= docsPerTx) {
        const kvs = [];
        for (let k = 0; k <= docsPerTx; k++) {
            const key = getRandomKey(2015, 2020);
            const value = getRandom(1, 20);
            rawKeys.push({key, value});
            kvs.push([key, value]);
        }
        console.time('tx');
        await db.doTransaction(async tn => {
            for ([key, value] of kvs) {
                // console.time('insert');
                await insert(tn, key, value);
                // console.timeEnd('insert');
                // if (i % 1000 === 0) {
                //     console.log("\n\n i=", i,"\n\n");
                // }
            }
        });
        console.timeEnd('tx');
    }
    console.timeEnd('total insert');
    // console.log('Added keys', rawKeys);

    // const lots = [
    //     { key: [ 2016, 6, 10 ], value: 15 },
    //     { key: [ 2017, 4, 8 ], value: 9 },
    //     { key: [ 2015, 2, 29 ], value: 2 },
    //     { key: [ 2015, 8, 17 ], value: 15 },
    //     { key: [ 2018, 10, 13 ], value: 2 },
    //     { key: [ 2016, 1, 16 ], value: 5 },
    //     { key: [ 2015, 7, 11 ], value: 8 },
    //     { key: [ 2016, 1, 27 ], value: 4 },
    //     { key: [ 2018, 1, 23 ], value: 17 },
    //     { key: [ 2015, 3, 25 ], value: 17 }
    // ]
    // for (kv of lots) {
    //     await db.doTransaction(async tn => {
    //         await insert(tn, kv.key, kv.value);
    //     });
    // }

}

const insertFromTop = async (tn, key, value) => {
    let currentVal = value; // if this k/v has been stored before we need to update this value at level 0 to be used through the other levels
    let level = 0;
    const existing = await getVal(tn, key, level);
    log('ff', key, currentVal, existing);
    if (existing) {
        currentVal = rereduce([existing, currentVal]);
    }
    await insertAtLevel(tn, key, currentVal, 0);

    for(level = MAX_LEVELS; level > 0; level--) {
        const previous = await getPrevious(tn, key, level);
        const tt = await tn.getRangeAll(
            ks.lastLessThan([level, key]),
            ks.firstGreaterOrEqual([level, key]),
        );
        log(level, 'key', key, 'full previous', tt, 'previous is', previous);
        if (hashCalc(key, level, LEVEL_FAN_POW)) {
            const lowerLevel = level - 1;
            // update previous node
            const newPrevRange = await getRange(tn, previous.key, key, lowerLevel);
            log('prevRange', newPrevRange, 'prevKey', previous, 'key', key);
            const prevValues = newPrevRange.map(kv => kv.value);
            const newPrevValue = rereduce(prevValues)
            if (newPrevValue !== previous.value) {
                await insertAtLevel(tn, previous.key, newPrevValue, level);
            }

            // calculate new nodes values
            const next = await getNext(tn, key, level);
            // const newRange = await getRange(tn, [...key, END], next.key, lowerLevel);
            const newRange = await getRange(tn, key, next.key, lowerLevel);
            const newValues = newRange.map(kv => kv.value);
            const newValue = rereduce(newValues);
            log('inserting at level', level, 'key', key, 'after', next, 'range', newRange);
            await insertAtLevel(tn, key, newValue, level);
        } else {
            const newValue = rereduce([previous.value, value]);
            log('rereduce at', level, 'key', previous.key, 'new value', newValue, 'prev value', previous.value);
            await insertAtLevel(tn, previous.key, newValue, level);
        }
    }


}

const insert = async (tn, key, value) => {
    let currentVal = value; // if this k/v has been stored before we need to update this value at level 0 to be used through the other levels
    for(let level = 0; level <= MAX_LEVELS; level++) {
        if (level === 0) {
            const existing = await getVal(tn, key, level);
            log('ff', key, currentVal, existing);
            if (existing) {
                currentVal = rereduce([existing, currentVal]);
            }
            await insertAtLevel(tn, key, currentVal, 0);
            continue;
        }
        const previous = await getPrevious(tn, key, level);
        const tt = await tn.getRangeAll(
            ks.lastLessThan([level, key]),
            ks.firstGreaterOrEqual([level, key]),
        );
        log(level, 'key', key, 'full previous', tt, 'previous is', previous);
        if (hashCalc(key, level, LEVEL_FAN_POW)) {
            const lowerLevel = level - 1;
            // update previous node
            const newPrevRange = await getRange(tn, previous.key, key, lowerLevel);
            log('prevRange', newPrevRange, 'prevKey', previous, 'key', key);
            const prevValues = newPrevRange.map(kv => kv.value);
            const newPrevValue = rereduce(prevValues)
            if (newPrevValue !== previous.value) {
                await insertAtLevel(tn, previous.key, newPrevValue, level);
            }

            // calculate new nodes values
            const next = await getNext(tn, key, level);
            // const newRange = await getRange(tn, [...key, END], next.key, lowerLevel);
            const newRange = await getRange(tn, key, next.key, lowerLevel);
            const newValues = newRange.map(kv => kv.value);
            const newValue = rereduce(newValues);
            log('inserting at level', level, 'key', key, 'after', next, 'range', newRange);
            await insertAtLevel(tn, key, newValue, level);
        } else {
            const newValue = rereduce([previous.value, value]);
            log('rereduce at', level, 'key', previous.key, 'new value', newValue, 'prev value', previous.value);
            await insertAtLevel(tn, previous.key, newValue, level);
        }
    }
};

const insertAtLevel = async (tn, key, value, level) => {
    log('INS', level, key, ':', value);
    return await tn.set([level, key], value);
};

const getRange = async (tn, start, end, level) => {
    const kvs = await tn.getRangeAll([level, start], [level, end]);

    return kvs.map(([[_level, key], value]) => {
        return {
            key,
            value
        };
    });
};

const getRangeInclusive = async (tn, start, end, level) => {
    const kvs = await tn.getRangeAll(
        ks.firstGreaterOrEqual([level, start]), 
        ks.firstGreaterThan([level, end])
        );

    return kvs.map(([[_level, key], value]) => {
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

const getVal = async (tn, key, level) => {
    return  await tn.get([level, key]);
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
            key: END,
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
        ks.firstGreaterThan([level, _endkey]),
        // ks.lastLessThan([level, _endkey]),
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

const getKeyAfter = async (tn, key, level, endkey) => {
    const _endkey = endkey ? endkey : END;
    const iter = await tn.snapshot().getRange(
        ks.firstGreaterThan([level, key]),
        ks.firstGreaterThan([level, _endkey]),
        // ks.lastLessThan([level, _endkey]),
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

const getGroupLevelEndKey = async (tn, groupLevel, level, startkey) => {
    const groupLevelKey = getGroupLevelKey(startkey, groupLevel);
    const end = groupLevelKey === null ? END : [...groupLevelKey, END];
    const iter = await tn.snapshot().getRange(
        ks.firstGreaterThan([level, groupLevelKey]),
        ks.firstGreaterOrEqual([level, end]),
        {reverse: true, limit: 1}
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
        ks.lastLessThan([level, key]),
        ks.firstGreaterThan([level, key]),
        {limit: 1, reverse: true}
    )
    
    //TODO: add a conflict key
    const item = await iter.next();
    if (item.done) {
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
                return a;
            });

            log(`Level ${level}`, keys);
            assert.equal(levelTotal, total, `Level ${level} - level total ${levelTotal} values not equal to level 0 ${total}`);
        }
    });

    return {
        total
    };
};

const keyToBinary = (one) => {
    let keyOne = one.key ? one.key : one;

    if (!Array.isArray(keyOne)) {
        keyOne = [keyOne];
    }


    return Buffer.from(keyOne);
}

const keysEqual = (one, two) => {
    if (one === null || two === null) {
        return false;
    }

    const binOne = keyToBinary(one);
    const binTwo = keyToBinary(two);

    return binOne.compare(binTwo) === 0;
}

const groupLevelEqual = (one, two, groupLevel) => {
    if (one === null || two === null) {
        return false
    }
    const levelOne = getGroupLevelKey(one.key, groupLevel);
    const levelTwo = getGroupLevelKey(two.key, groupLevel);

    return keysEqual(levelOne, levelTwo);
};

const keyGreater = (one, two) => {
    if (!one || !two) {
        return false;
    }

    const binOne = keyToBinary(one);
    const binTwo = keyToBinary(two);

    // key two comes after
    return binOne.compare(binTwo) === -1;
}

const getGroupLevelKey = (key, groupLevel) => {
    if (groupLevel === 0) {
        return null
    }

    if (!Array.isArray(key)) {
        return key;
    }

    if (key.length <= groupLevel) {
        return key;
    }

    return key.slice(0, groupLevel);
};

const collateRereduce = (acc, groupLevel) => {
    const acc1 = acc.reduce((acc, kv) => {
        const key = getGroupLevelKey(kv.key, groupLevel);

        if (!acc[key]) {
            acc[key] = {
                key,
                values: []
            };
        }

        acc[key].values.push(kv.value);
        return acc;
    }, {});

    return Object.values(acc1).reduce((acc, kv) => {
        const values = kv.values;
        const key = kv.key;
        const result = rereduce(values);

        acc.push({
            key,
            value: result
        });

        return acc;
    }, []);
};

const getNextRangeAndLevel = async (tn, groupLevel, level, prevLevel, startkey, endkey) => {
    let groupEndkey = await getGroupLevelEndKey(tn, groupLevel, 0, startkey.key);
    log('groupendkey', groupEndkey, 'end', endkey, keyGreater(endkey, groupEndkey));
    if (keyGreater(endkey, groupEndkey)) {
        groupEndkey = endkey;
    }
    log('LEVEL 0 searc', startkey, groupEndkey);
    const levelRanges = [{
        level: 0,
        start: startkey,
        end: groupEndkey
    }];
    for (let i = 0; i < MAX_LEVELS; i++) {
        log('next start', startkey, 'i', i);
        // look 1 level above
        let nearestLevelKey = await getKeyOrNearest(tn, startkey.key, i + 1, endkey.key);
        log('nearest', nearestLevelKey, "level", i + 1, "start", startkey, "grouplevelequal", groupLevelEqual(startkey, nearestLevelKey, groupLevel));

        if (keysEqual(nearestLevelKey, startkey)) {
            const groupLevelEndKey = await getGroupLevelEndKey(tn, groupLevel, i + 1, nearestLevelKey.key);
            log('CALCUP1', 'nearest', nearestLevelKey, 'after', groupLevelEndKey, 'level', i);
            if (groupLevelEndKey !== null) {
                if (keyGreater(endkey, groupLevelEndKey)) {
                    log('grouplevel great than endkey', endkey, groupLevelEndKey);
                    // exceeded the range at this level we can't go further
                    break;
                }
                // end of section have to do the read at level 0
                if (keysEqual(nearestLevelKey, groupLevelEndKey)) {
                    return [0, nearestLevelKey, nearestLevelKey];
                }

                levelRanges.push({
                    level: i + 1,
                    start: nearestLevelKey,
                    end: groupLevelEndKey
                });
                continue;
            }
        } else if (nearestLevelKey !== null && groupLevelEqual(startkey, nearestLevelKey, groupLevel)) {
            log('querying to nearest level up', startkey, nearestLevelKey);
            return [i, startkey, nearestLevelKey];
        } 

        break;
    }

    
    log('gone to far', JSON.stringify(levelRanges, ' ', null));
    const out = levelRanges.pop();
    return [out.level, out.start, out.end]
};

const traverse = async (tn, level, prevLevel, current, endkey, groupLevel, acc) => {
    if (level < 0) {
        throw new Error("gone to low");
    }
    const [rangeLevel, rangeStart, rangeEnd] = await getNextRangeAndLevel(tn, groupLevel, level, prevLevel, current, endkey);
    log('RANGE QUERY, level', rangeLevel, 'start', rangeStart, 'end', rangeEnd);

    stats[rangeLevel].push([rangeStart.key, rangeEnd.key]);
    const results = await getRangeInclusive(tn, rangeStart.key, rangeEnd.key, rangeLevel);
    log('RESULTS', results, 'start', rangeStart.key, 'end', rangeEnd.key);
    // test with rangeEnd always next startkey
    let nextStartKey = results[results.length - 1];
    const useableResults = results.slice(0, results.length -1);
    acc = [...acc, ...useableResults];
    if (rangeLevel === 0) {
        acc.push(nextStartKey);
        log('COLLATE', acc);
        const reducedResults = collateRereduce(acc, groupLevel);
        acc = reducedResults;
        nextStartKey = await getKeyAfter(tn, nextStartKey.key, rangeLevel, endkey.key);

        if (!groupLevelEqual(rangeEnd, nextStartKey)) {
            //should stream results for a common group at this point
        }
    }

    log('END EQUAL', 'rangend', rangeEnd, 'end', endkey, keysEqual(rangeEnd, endkey));
    if ((keysEqual(rangeEnd, endkey) || nextStartKey === null) && rangeLevel === 0) {
        return acc;
    }

    log('moving next rangeLevel', rangeLevel, 'newStart', nextStartKey, acc);
    return traverse(tn, 0, rangeLevel, nextStartKey, endkey, groupLevel, acc);
}

const formatResult = (results) => {
    return {
        rows: results
    };
};

const query = async (opts) => {
    resetStats();
    return await db.doTransaction(async tn => {
        let endkey = {key: END, value: 0};
        let startkey = {key: '0', value: 0};

        if (opts.startkey) {
            startkey = await getKeyOrNearest(tn, opts.startkey, 0);
            if (!startkey) {
                return false; //startkey out of range;
            }
            log('startkey', opts.startkey, startkey);
        }

        if (opts.endkey) {
            endkey = await getKeyOrFirstBefore(tn, opts.endkey, 0);
            log('endkey', opts.endkey, endkey);
        }

        if (opts.group) {
            const results = await getRangeInclusive(tn, startkey.key, endkey.key, 0);
            return formatResult(results);
        }

        if (opts.group_level === 0 && !opts.startkey && !opts.endkey) {
                const results = await getRange(tn, '0', END, MAX_LEVELS);
                if (results.length > 1) {
                    const vals = results.map(kv => kv.value);
                    const total = rereduce(vals);
                    return formatResult([{
                        key: null,
                        value: total
                    }]);
                }

                return formatResult([{
                    key: null,
                    value: results[0].value
                }]);
        }


        const results = await traverse(tn, 0, 0, startkey, endkey, opts.group_level, []);
        log('final results', results);
        console.log('query stats', util.inspect(stats, {depth: null}));
        return formatResult(results);
    });
};


const simpleQueries = async () => {
    let result = {};
    result = await query({group_level: 0});
    assert.deepEqual(result, {
        rows: [{
            key: null,
            value: 68
        }]
    });

    result = await query({group_level:0, startkey: [2018, 3, 2]});
    assert.deepEqual(result, {
        rows: [{
            key: null,
            value: 31
        }]
    });

    result = await query({
        group_level:0,
        startkey: [2018, 3, 2],
        endkey: [2019, 5, 1]
    });
    assert.deepEqual(result, {
        rows: [{
            key: null,
            value: 31
        }]
    });

    result = await query({
        group_level: 0,
        startkey: [2018, 03, 2],
        endkey: [2019, 03, 2],

    })

    assert.deepEqual(result, {
        rows: [{
            key: null,
            value: 18
        }]
    });

    result = await query({
        group_level: 1,
        startkey: [2017, 4, 1],
        endkey: [2018, 3, 1],
    })

    assert.deepEqual(result, {
        rows: [
        {
            key: [2017],
            value: 22
        },
        {
            key: [2018],
            value: 6
        }
    ]
    });

    result = await query({
        group_level: 1,
        startkey: [2017, 4, 1],
        endkey: [2019, 03, 2],

    })

    assert.deepEqual(result, {
        rows: [
        {
            key: [2017],
            value: 22
        },
        {
            key: [2018],
            value: 20
        },
        {
            key: [2019],
            value: 4
        }
    ]
    });

    result = await query({
        group_level: 1,
        startkey: [2017, 4, 1],
        endkey: [2019, 05, 1],

    })

    assert.deepEqual(result, {
        rows: [
        {
            key: [2017],
            value: 22
        },
        {
            key: [2018],
            value: 20
        },
        {
            key: [2019],
            value: 17
        }
    ]
    });

    result = await query({
        group: true,
        startkey: [2018, 5, 1],
        endkey: [2019, 4, 1],
    });

    assert.deepEqual(result, {rows: [
        {key: [2018,5,1], value: 7},
        {key: [2019,3,1], value: 4},
        {key: [2019,4,1], value: 6}
    ]})
    log('SIMPLE DONE');
};

const queryLevel0 = async (opts) => {
    return await db.doTransaction(async tn => {
        let endkey = {key: END, value: 0};
        let startkey = {key: '0', value: 0};

        if (opts.startkey) {
            startkey = await getKeyOrNearest(tn, opts.startkey, 0);
        }

        if (opts.endkey) {
            endkey = await getKeyOrFirstBefore(tn, opts.endkey, 0);
        }
        const results = await getRangeInclusive(tn, startkey.key, endkey.key, 0);
        const acc1 = collateRereduce(results, opts.group_level); 
        return formatResult(acc1);
    });
}

const largeQueries = async () => {
    let result;
    const [startkey, endkey] = await db.doTransaction(async tn => {
        const start = await getKeyAfter(tn, '0', 0);
        const end = await getPrevious(tn, END, 0);

        return [start.key, end.key];
    });

    for (let i = 0; i < 100; i++) {
        // const endkey = getRandomKey(startkey[0] + 1, 2020);
        const opts = {
            group_level: 1,
            startkey,//: [2016, 1, 7],
            endkey//: [2018, 7, 8]
        };
        console.log('range', startkey, endkey);
        console.time('query');
        result = await query(opts);
        console.timeEnd('query');

        console.time('level0');
        const level1Result = await queryLevel0(opts);
        console.timeEnd('level0');
        assert.deepEqual(result, level1Result);
    }
};

const run = async () => {
    // await clear();
    // await create();
    // await print();
    // await simpleQueries();
    // await createLots();
    // await print();
    await largeQueries();
};

run();