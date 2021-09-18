amqp = require('amqplib');

(async () => {
    try {
        const args = process.argv.slice(2);
        const workerNum = args[0];

        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await channel.assertQueue('rpc_queue', { durable: false });
        await channel.assertQueue('from_worker', { durable: false });
        await channel.assertExchange('to_worker', 'fanout', { durable: false });

        let roads = [];

        channel.consume(
            'from_worker', 
            workerMsg => { 
                console.log(" [x] Master received: %s", workerMsg.content.toString());
                console.log("Current array: " + roads + "\n");
                channel.ack(workerMsg);

                roads.push(workerMsg.content.toString());

                if (roads.length == workerNum) {
                    const json = JSON.stringify(roads);
                    console.log("Sending to client " + json);

                    channel.sendToQueue(
                        workerMsg.properties.replyTo,
                        Buffer.from(json)
                    );

                    roads = [];
                }

            }
        );

        channel.consume(
            'rpc_queue',
            async (clientMsg) => {
                let letter = clientMsg.content.toString();
                channel.ack(clientMsg);

                console.log('*** Broadcasting letter: ' + letter + '   ***\n');

                channel.publish(
                    'to_worker', 
                    '', 
                    Buffer.from(letter), 
                    { replyTo: clientMsg.properties.replyTo }
                );
            }
        );

    } catch (err) {
        console.log(err);
    }
})();