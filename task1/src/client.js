const amqp = require('amqplib');

function isLetter(str) {
    return str.length === 1 && str.match(/[a-z]/i);
}

(async () => {
    try {
        const args = process.argv.slice(2);
        const letter = args[0];

        if (!isLetter(letter)) {
            console.log(`\n${letter} is not a letter\n`);
            return 0;
        }

        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        const queueFromMaster = await channel.assertQueue('', { exclusive: true });
        await channel.assertQueue('rpc_queue');

        channel.on('error', err => {
            console.log(err);
        });

        channel.consume(
            queueFromMaster.queue,
            msg => {
                channel.ack(msg);

                if (JSON.parse(msg.content.toString()).isError) {
                    console.log("Server error!")
                    return;
                }

                const content = JSON.parse(msg.content).map(item => 
                    item == '' ? null : JSON.parse(item)
                );

                content.forEach(item => {
                    if (!item) {
                        return;
                    }
                    console.log(` * ${item.city} * `);
                    console.log(`Number of roads: ${item.roadCount}`);
                    item.roads.forEach(road => console.log(`- ${road}`));
                    console.log('\n')
                })
            }
        );

        channel.sendToQueue(
            'rpc_queue',
            Buffer.from(letter),
            { replyTo: queueFromMaster.queue }
        );

    } catch (err) {
        console.log(err);
    }
})();