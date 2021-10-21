const { spawn } = require('child_process');

const config = require('../config');

const maxFriendsNum = config.maxFriendsNum;
const latencyDistrib = config.latencyDistrib;
const baseLatencyMs = config.baseLatencyMs;

function run() {
    const len = Math.min(maxFriendsNum, latencyDistrib.length);
    for (let i = 0; i < len; ++i) {
        const child = spawn(process.argv[0], ['src/friend.js', i, latencyDistrib[i]]);

        console.log(`friend #${i} - latency ${latencyDistrib[i] * baseLatencyMs}`);

        child.stdout.on('data', (data) => {
            console.log(`friend #${i}: ${data.slice(0, -1)}`);
        });
    
        child.on('error', (error) => {
            console.error(`friend #${i}: ${error.message}`);
        });
          
        child.on('close', (code) => {
            console.log(`friend #${i} exited with code ${code}`);
        });
    }
}

run();