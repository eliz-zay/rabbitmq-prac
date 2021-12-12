const amqp = require('amqplib');

const config = require('../config.json');

const baseLatencyMs = config.baseLatencyMs;
const sailorsNum = config.sailorsNum;

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function clockIsValid(msg, clock) {
    const { sender, clock: msgClock } = msg;
    return (
        clock[sender] === msgClock[sender] - 1
        && clock.every((val, i) => 
            i === sender || clock[i] >= msgClock[i]
        )
    );
}

function updateClock(msgClock, clock) {
    const newClock = new Array(clock.length);
    for (let i = 0; i < clock.length; ++i) {
        newClock[i] = Math.max(clock[i], msgClock[i]);
    }

    return newClock;
}

function bcastForecast(procId, msg, clock, channel) {
    const resp = JSON.stringify({
        clock,
        isRequest: false,
        sender: procId,
        asker: msg.sender
    });

    channel.sendToQueue(
        'to_bcast',
        Buffer.from(resp),
    );
}

function processMsgs(msg, msgQueue, sortedMsgs, id, clock, channel) {
    if (!clockIsValid(msg, clock)) {
        msgQueue.add(msg);
        return { msgQueue, sortedMsgs };
    }

    sortedMsgs.push(msg);
    clock = updateClock(msg.clock, clock);

    if (msg.isRequest) {
        ++clock[id];
        bcastForecast(id, msg, clock, channel);
    }

    let success = true;
    while (success) {
        success = false;
        msgQueue.forEach(m => {
            if (clockIsValid(m, clock)) {
                success = true;
                msgQueue.pop(m);
                sortedMsgs.push(m);
                clock = updateClock(m.clock, clock);

                if (msg.isRequest) {
                    ++clock[id];
                    bcastForecast(id, msg, clock, channel);
                }
            }
        });
    }

    return { msgQueue, sortedMsgs };
}

async function run() {
    try {
        const args = process.argv.slice(2);
        const id = +args[0];
        const latency = (id + 1) * baseLatencyMs;

        let clock = new Array(sailorsNum).fill(0);
        let sortedMsgs = [];
        const rcvOrderMsgs = [];
        let msgQueue = new Set();

        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await channel.assertExchange('bcast', 'fanout');
        await channel.assertQueue('to_bcast', { durable: true });
        const queueFromBcast = await channel.assertQueue('', { exclusive: true, autoDelete: true });

        channel.bindQueue(queueFromBcast.queue, 'bcast', '');

        channel.consume(
            queueFromBcast.queue,
            async rawMsg => {
                channel.ack(rawMsg);

                const msg = JSON.parse(rawMsg.content.toString());

                rcvOrderMsgs.push(msg);
                ({ msgQueue, sortedMsgs } = processMsgs(msg, msgQueue, sortedMsgs, id, clock, channel));
            }
        );

        await sleep(latency);

        ++clock[id];
        const reqMsg = JSON.stringify({
            clock,
            isRequest: true,
            sender: id,
        });

        channel.sendToQueue(
            'to_bcast',
            Buffer.from(reqMsg),
        );
    } catch (err) {
        console.log(err);
    }
}

run();