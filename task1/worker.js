amqp = require('amqplib');

(async () => {
    try {
        let args = process.argv.slice(2);
        let number = args[0];

        console.log('*** Responding number: ' + number + '   ***\n');

        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await channel.assertQueue('from_worker', { durable: false });

        await channel.assertExchange('to_worker', 'fanout', { durable: false });
        const q = await channel.assertQueue('', { exclusive: true });
        channel.bindQueue(q.queue, 'to_worker', '');

        channel.consume(
            q.queue,
            msg => { 
                console.log(" [x] Received: %s", msg.content);
                channel.ack(msg);

                console.log(msg.properties)

                channel.sendToQueue(
                    "from_worker", 
                    Buffer.from(number),
                    { replyTo: msg.properties.replyTo}
                );
            }
        );

    } catch (err) {
        console.log(err);
    }
})();