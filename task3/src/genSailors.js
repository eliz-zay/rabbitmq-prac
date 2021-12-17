const { spawn } = require('child_process');

const config = require('../config');

const {
    sailorsNum,
    baseLatencyMs,
    simulatorBufferLen
 } = config;

 function parseArgs() {
    if (sailorsNum <= 0 || baseLatencyMs <= 0 || simulatorBufferLen <= 0) {
        console.log('Invalid args');
        process.exit(0);
    }
}

function run() {
    parseArgs();

    for (let i = 0; i < sailorsNum; ++i) {
        const child = spawn(process.argv[0], ['src/sailor.js', i]);

        console.log(`sailor #${i} - latency ${(i + 1) * baseLatencyMs} ms`);

        child.stdout.on('data', (data) => {
            console.log(`\nsailor #${i}:\n${data.slice(0, -1)}`);
        });
    
        child.on('error', (error) => {
            console.error(`sailor #${i}: ${error.message}`);
        });
          
        child.on('close', (code) => {
            console.log(`sailor #${i} exited with code ${code}`);
        });
    }
}

run();