const amqp = require('amqplib');

const config = require('../config.json');

const { simulatorBufferLen: bufferLen, sailorsNum } = config;

function bcast(msgs, channel) {
    console.log('sending...\n');

    msgs = msgs.reverse();
    msgs.forEach(m => {
        channel.publish(
            'bcast',
            '',
            Buffer.from(JSON.stringify(m)),
        );
    });
}

async function run() {
    try {
        let msgs = [];
        let lastMsgTime;

        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await channel.assertExchange('bcast', 'fanout');
        await channel.assertQueue('to_bcast', { durable: true });

        const timerId = setInterval(() => {
            if (!lastMsgTime) {
                return;
            }

            const now = new Date();
            if (msgs.length && now.getTime() - lastMsgTime.getTime() >= 2000) {
                bcast(msgs, channel);
                msgs.length = 0;
            }
        }, 2000);

        channel.consume(
            'to_bcast',
            async rawMsg => {
                channel.ack(rawMsg);

                const msg = JSON.parse(rawMsg.content.toString())
                msgs.push(msg);
                lastMsgTime = new Date();
                console.log(msg);
                
                if (msgs.length === bufferLen) {
                    bcast(msgs, channel);
                    msgs.length = 0;
                }
            }
        );
    } catch (err) {
        console.log(err);
    }
}

run();