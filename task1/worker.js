const amqp = require('amqplib');
const osmread = require('osm-read');

async function parseOSM(file, firstLetter = null) {
    return new Promise((res, rej) => {
        let roads = [];
        osmread.parse({
            filePath: file,
            way: way => {
                if (way.tags.highway && way.tags.name && (!firstLetter || firstLetter && way.tags.name[0] == firstLetter)) {
                    roads.push(way.tags.name);
                }
            },
            endDocument: () => {
                res(roads);
            },
            error: err => {
                rej(new Error(err))
            }
        });
    });
}

(async () => {
    try {
        const args = process.argv.slice(2);
        const cityFile = args[0];

        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await channel.assertQueue('from_worker', { durable: false });

        await channel.assertExchange('to_worker', 'fanout', { durable: false });
        const q = await channel.assertQueue('', { exclusive: true });
        channel.bindQueue(q.queue, 'to_worker', '');

        channel.consume(
            q.queue,
            async msg => { 
                channel.ack(msg);

                const roads = await parseOSM(cityFile, msg.content.toString());
                const response = JSON.stringify({
                    city: cityFile,
                    roadCount: roads.length,
                    roads: roads,
                });

                channel.sendToQueue(
                    "from_worker", 
                    Buffer.from(response),
                    { replyTo: msg.properties.replyTo}
                );
            }
        );

    } catch (err) {
        console.log(err);
    }
})();