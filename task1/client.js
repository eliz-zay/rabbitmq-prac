amqp = require('amqplib');

(async () => {
    try {
        const args = process.argv.slice(2);
        const letter = args[0];

        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        const q = await channel.assertQueue('', { exclusive: true });

        channel.consume(
            q.queue,
            msg => {
                console.log(' [x] Client recieved');
                console.log(msg.content.toString());
                channel.ack(msg);
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