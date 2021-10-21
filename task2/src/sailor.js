const amqp = require('amqplib');

const config = require('../config.json');

const period = config.sailorPeriodMs;

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function run() {
    try {
        let counter = 0;
        let msgs = [];

        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await channel.assertExchange('to_friends', 'fanout');
        await channel.assertQueue('to_sailor', { durable: true });

        channel.consume(
            'to_sailor',
            async msg => {
                channel.ack(msg);

                const jsonMsg = JSON.parse(msg.content.toString());

                if (jsonMsg.counter == counter) {
                    msgs.push(jsonMsg);
                }
            }
        )

        while (true) {
            const request = JSON.stringify({ counter });

            channel.publish(
                'to_friends', 
                '', 
                Buffer.from(request),
            );

            await sleep(period);

            console.log(`\n#${counter}`);
            for (let i = 0; i < msgs.length; ++i) {
                const msg = msgs[i];
                console.log(`from #${msg.id}: ${msg.content} at ${msg.time}`);
            }

            msgs = [];
            ++counter;
        }
    } catch (err) {
        console.log(err);
    }
}

run();