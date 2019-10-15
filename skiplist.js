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
    [[2017,3,1], 9],
    [[2017,4,15], 6], //*
    [[2017,4,1], 7], //*
    [[2017,5,1], 9], //*
    [[2018,3,1], 6], //*
    [[2018,4,1], 7], //* 
    [[2018,5,1], 7], //*
    [[2019,3,1], 4], //*
    [[2019,4,1], 6], //* 
    [[2019,5,1], 7]
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
                console.log('prevRange', newPrevRange)
                const prevValues = newPrevRange.map(kv => kv.value);
                const newPrevValue = rereduce(prevValues)
                if (newPrevValue !== previous.value) {
                    await insertAtLevel(tn, previous.key, newPrevValue, level);
                }

                // calculate new nodes values
                const next = await getNext(tn, previous.key, level);
                const newRange = await getRange(tn, key, next.key, lowerLevel);
                const newValues = newRange.map(kv => kv.value);
                const newValue = rereduce([...newValues, value])
                await insertAtLevel(tn, key, newValue, level);
            } else {
                const newValue = rereduce([previous.value, value]);
                await insertAtLevel(tn, previous.key, newValue, level);
            }
        }
    })
};

const insertAtLevel = async (tn, key, value, level) => {
    console.log('INS', level, key, ':', value);
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

const getKeyOrFirstBefore = async (tn, key, level) => {
    const iter = await tn.snapshot().getRange(
        ks.lastLessThan([level, key]),
        ks.firstGreaterThan([level, key]),
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

const keysEqual = (one, two) => {
    if (one === null || two === null) {
        return false;
    }

    // BLAH!!!
    // console.log('cc', one.key, two.key);
    return JSON.stringify(one.key) === JSON.stringify(two.key)
}

const getNewStartKey = (currentStartkey, results) => {
    if (results.length >= 2) {
        return results[results.length - 2];
    }

    return currentStartkey;
}

const kvs1 = [
    [[2017,3,1], 9],
    [[2017,4,15], 6],
    [[2017,4,1], 7],
    [[2017,5,1], 9],
    [[2018,3,1], 6],
    [[2018,4,1], 7], //* 
    [[2018,5,1], 7], //*
    [[2019,3,1], 4], //*
    [[2019,4,1], 6], //* 
    [[2019,5,1], 7]
  ]

/*
    Start
    can we go up?
    Yes, so back to start
    No, get range of results for startkey, to nearest on up level
    Reduce results - exclude final result if not at level 0
    currentkey = last key in reduce
    currentkey = endkey and level 0 COMPLETE
    go level down currentkey = endkey
    back to start but can't go up
*/

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

    return key.slice(0, groupLevel + 1);
};

const collateRereduce = (acc, groupLevel) => {
    console.log('rr', acc);
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

    console.log('AA', acc1);
    return Object.values(acc1).reduce((acc, kv) => {
        const values = kv.values;
        const key = kv.key;
        const result = rereduce(values);

        // for this we just accumulating the kv's. In CouchDB we can stream some of the results back at this point
        acc.push({
            key,
            value: result
        });

        return acc;
    }, []);
};

const calcCanGoUp = async (tn, level, prevLevel, current, endkey) => {
    if (prevLevel > level) {
        return [false, endkey];
    }

    const nearestLevelUp = await getKeyOrNearest(tn, current.key, level + 1, endkey.key);
    if (keysEqual(nearestLevelUp, current)) {
        return [true, current];
    }

    const rangeEnd = nearestLevelUp === null ? endkey : nearestLevelUp;
    return [false, rangeEnd];
}

const traverse = async (tn, level, prevLevel, current, endkey, groupLevel, acc) => {
    const [canGoUp, rangeEnd] = await calcCanGoUp(tn, level, prevLevel, current, endkey);
    console.log('traverse', canGoUp, rangeEnd, level, prevLevel, current, endkey, acc);

    if (canGoUp) {
        console.log('TRAVERSE to', level + 1);
        return await traverse(tn, level + 1, level, current, endkey, groupLevel, acc);
    }

    const results = await getRangeInclusive(tn, current.key, rangeEnd.key, level);
    console.log('RESULTS', results);
    const lastResult = results[results.length - 1];
    const useableResults = results.slice(0, results.length -1);
    acc = [...acc, ...useableResults];

    console.log('end range compare', lastResult, endkey, keysEqual(lastResult, endkey));
    if (keysEqual(lastResult, endkey) && level === 0) {
        console.log('final key', lastResult);
        acc.push(lastResult);
        return collateRereduce(acc, groupLevel);
    }
    // console.log('ACC', acc);
    console.log('Going down', level -1, lastResult, endkey, acc);

    return traverse(tn, level -1, level, lastResult, endkey, groupLevel, acc);
}

const formatResult = (results) => {
    return {
        rows: results
    };
};

const query = async (opts) => {
    return await db.doTransaction(async tn => {
        let endkey = {key: END, value: 0};
        let startkey = {key: '0', value: 0};

        if (opts.startkey) {
            startkey = await getKeyOrNearest(tn, opts.startkey, 0);
            console.log('startkey', opts.startkey, startkey);
            // startkey.key = JSON.stringify(opts.startkey);
        }

        if (opts.endkey) {
            endkey = await getKeyOrFirstBefore(tn, opts.endkey, 0);
            console.log('endkey', opts.endkey, endkey);
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
        console.log('final results', results);
        return formatResult(results);
    });
};

const run = async () => {
    await clear();
    await create();
    await print();

    // const result = await query({group_level: 0});
    // assert.deepEqual(result, {
    //     rows: [{
    //         key: null,
    //         value: 68
    //     }]
    // });

    // const result2 = await query({group_level:0, startkey: [2018, 3, 2]});
    // assert.deepEqual(result2, {
    //     rows: [{
    //         key: null,
    //         value: 31
    //     }]
    // });

    const resultSE = await query({
        group_level:0,
        startkey: [2018, 3, 2],
        endkey: [2019, 4, 1]
    });
    assert.deepEqual(resultSE, {
        rows: [{
            key: null,
            value: 24
        }]
    });

    console.log('r2');
    const result3 = await query({
        group_level: 0,
        startkey: [2018, 03, 2],
        endkey: [2019, 03, 2],

    })

    assert.deepEqual(result3, {
        rows: [{
            key: null,
            value: 18
        }]
    });

    // const result4 = await query({
    //     group_level: 1,
    //     startkey: [2017, 04, 1],
    //     endkey: [2019, 03, 2],

    // })

    // assert.deepEqual(result4, {
    //     rows: [
    //     {
    //         key: [2017],
    //         value: 11
    //     },
    //     {
    //         key: [2018],
    //         value: 20
    //     },
    //     {
    //         key: [2019],
    //         value: 10
    //     }
    // ]
    // });

    // const result4 = await query({
    //     group: true,
    //     startkey: [2018, 5, 1],
    //     endkey: [2019, 4, 1],
    // });

    // assert.deepEqual(result4, {rows: [
    //     {key: '[2018,5,1]', value: 7},
    //     {key: '[2019,3,1]', value: 4},
    //     {key: '[2019,4,1]', value: 6}
    // ]})
};

run();