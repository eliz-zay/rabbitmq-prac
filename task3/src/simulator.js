const amqp = require('amqplib');

const config = require('../config.json');

const bufferLen = +config.simulatorBufferLen;

async function run() {
    try {
        let msgs = [];

        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await channel.assertExchange('bcast', 'fanout');
        await channel.assertQueue('to_bcast', { durable: true });

        channel.consume(
            'to_bcast',
            async rawMsg => {
                channel.ack(rawMsg);

                const msg = JSON.parse(rawMsg.content.toString())
                msgs.push(msg);

                console.log(msg);
                
                if (msgs.length === bufferLen) {
                    // msgs = msgs.reverse();
                    msgs.forEach(m => {
                        channel.publish(
                            'bcast',
                            '',
                            Buffer.from(JSON.stringify(m)),
                        );
                    });

                    msgs.length = 0;
                }
            }
        )
    } catch (err) {
        console.log(err);
    }
}

run();