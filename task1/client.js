amqp = require('amqplib');

(async () => {
    try {
        const args = process.argv.slice(2);
        const letter = args[0];

        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        const q = await channel.assertQueue('', { exclusive: true });
        await channel.assertQueue('rpc_queue', { durable: true });

        channel.consume(
            q.queue,
            msg => {
                channel.ack(msg);

                console.log(' [x] Client recieved');
                const content = JSON.parse(msg.content).map(item => JSON.parse(item))
                console.log(content)
            }
        );

        channel.sendToQueue(
            'rpc_queue',
            Buffer.from(letter),
            { replyTo: q.queue }
        );

    } catch (err) {
        console.log(err);
    }
})();