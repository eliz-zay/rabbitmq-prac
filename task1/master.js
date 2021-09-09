amqp = require('amqplib');

(async () => {
    try {
        let args = process.argv.slice(2);
        let letter = args[0];

        console.log('*** Broadcasting letter: ' + letter + '   ***\n');

        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        const q = await channel.assertQueue('', { durable: true });
        channel.consume(
            q.queue, 
            msg => { console.log(" [x] Master received: %s", msg.content.toString()); }, 
            { noAck: true }
        );
      
        await channel.assertExchange('to_worker', 'fanout', { durable: false });
        channel.publish(
            'to_worker', 
            '', 
            Buffer.from(letter), 
            { replyTo: q.queue }
        );

        console.log(' [x] Sent ' + letter + ' from master');
      
        // setTimeout(() => {
        //     connection.close();
        //     process.exit(0);
        // }, 500);

    } catch (err) {
        console.log(err);
    }
})();