const MAX_LEVELS = 10;
const LEVEL_FAN_POW = 4; // 2^X per level or (1 / 2^X) less than previous level
const items = 300000;

const skipList = {};
const stats = {}

const reset = () => {
    for (let level = 0; level <= MAX_LEVELS; level++) {
        skipList[level] = [];
        stats[level] = 0;
    }
}

reset();

const calcStats = (pow) => {
    const level0 = skipList[0].length;
    for (let level = 0; level <= MAX_LEVELS; level++) {
        let prev = items;
        if (level > 0) {
            prev = skipList[level - 1].length;
        }
        const current = skipList[level].length;
        stats[level] = {
            items: current,
            ratio0: (current / level0).toFixed(4),
            ratio: (current / prev).toFixed(4)
        }
    }
    console.log('ratio should be', (1/Math.pow(2, pow)).toFixed(3));
    console.log('stats', stats);
}

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
    const keyHash = hashCode(key);
    // console.log(keyHash);
    const out = (keyHash & ((1 << (level * pow)) - 1));
    // console.log(out, out != 0);
    // console.log(((1 << (level * LEVEL_FAN_POW)) - 1));
    if (out !== 0) {
        return false;
    }

    return true;
    // if ((keyHash & ((1 << (level * LEVEL_FAN_POW)) - 1)) != 0) {
    //     console.log(level, "atomic");
    // } else {
    //     console.log(level, "add");
    // }
}

const coinFlip = () => {
    return (Math.floor(Math.random() * 2) == 0);
};

for (let pow = 1; pow < 6; pow++) {
    console.log('POW', pow);
    for(let i = 0; i < items; i++) {
        for (let level = 0; level <= MAX_LEVELS; level++) {
            if (hashCalc(i.toString(), level, pow)) {
                skipList[level].push(i);
            }
        } 
    }
    calcStats(pow);
    reset();
}
for(let i = 0; i < items; i++) {
    for (let level = 0; level <= MAX_LEVELS; level++) {
        if (level === 0 || coinFlip()) {
            skipList[level].push(i);
        } else {
            break;
        }
    } 
}


// console.log('done', skipList);
