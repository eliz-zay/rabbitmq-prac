const amqp = require('amqplib');

const config = require('../config.json');

const baseLatencyMs = config.baseLatencyMs;

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function run() {
    try {
        const args = process.argv.slice(2);

        const id = args[0];
        const latency = args[1] * baseLatencyMs;

        let hasMsgFromFriend = false;
        let msgTime = undefined;

        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await channel.assertExchange('to_friends', 'fanout');
        await channel.assertExchange('among_friends', 'fanout');

        const queueFromSailor = await channel.assertQueue('', { exclusive: true });
        const queueFromFriends = await channel.assertQueue('', { exclusive: true });
        await channel.assertQueue('to_sailor', {  durable: true  });

        channel.bindQueue(queueFromSailor.queue, 'to_friends', '');
        channel.bindQueue(queueFromFriends.queue, 'among_friends', '');

        channel.consume(
            queueFromFriends.queue,
            async msg => {
                channel.ack(msg);
                hasMsgFromFriend = true;
                msgTime = new Date();

                const jsonMsg = JSON.parse(msg.content.toString());

                console.log(` msg from #${jsonMsg.id}`);
            }
        );

        channel.consume(
            queueFromSailor.queue,
            async msg => { 
                channel.ack(msg);
                try {
                    const content = msg.content.toString();
                    const { counter } = JSON.parse(content);

                    hasMsgFromFriend = false;

                    await sleep(latency);

                    const now = new Date();
                    const timeDiff = hasMsgFromFriend ? (now.getTime() - msgTime.getTime()) : undefined;

                    if (hasMsgFromFriend) {
                        console.log(`time diff: ${timeDiff}`);
                    }

                    if (!hasMsgFromFriend || timeDiff < baseLatencyMs / 10) {
                        console.log(` *** i send ***`);
                        channel.publish(
                            'among_friends',
                            '',
                            Buffer.from(JSON.stringify({ id }))
                        );

                        const forecast = JSON.stringify({
                            counter,
                            time: new Date().toISOString(),
                            id,
                            content: `about ${counter}`,
                        });

                        channel.sendToQueue(
                            'to_sailor',
                            Buffer.from(forecast),
                        );
                    }

                } catch (err) {
                    console.log(err)
                }
            }
        );

    } catch (err) {
        console.log(err);
    }
}

run();