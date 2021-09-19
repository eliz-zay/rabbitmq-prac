const amqp = require('amqplib');
const osmread = require('osm-read');

async function parseOSM(file, firstLetter = null) {
    return new Promise((resolve, reject) => {
        let roads = [];
        osmread.parse({
            filePath: file,
            way: way => {
                if (
                    way.tags.highway && 
                    way.tags.name && 
                    (!firstLetter || firstLetter && way.tags.name[0].toLowerCase() == firstLetter.toLowerCase()) 
                ) {
                    roads.push(way.tags.name);
                }
            },
            endDocument: () => {
                resolve(roads);
            },
            error: err => {
                reject(new Error(err))
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

        await channel.assertQueue('from_worker');
        await channel.assertExchange('to_worker', 'fanout');
        
        const queueFromMaster = await channel.assertQueue('', { exclusive: true });
        channel.bindQueue(queueFromMaster.queue, 'to_worker', '');

        channel.consume(
            queueFromMaster.queue,
            async msg => { 
                channel.ack(msg);

                try {
                    const roads = await parseOSM(cityFile, msg.content.toString());
                    const response = JSON.stringify({
                        city: cityFile.split('/')[1].split('.')[0], // cityFile: data/city.xml
                        roadCount: roads.length,
                        roads: roads,
                    });

                    channel.sendToQueue(
                        "from_worker", 
                        Buffer.from(response),
                        { replyTo: msg.properties.replyTo}
                    );

                } catch (err) {
                    console.log(err)

                    const json = JSON.stringify({ isError: true });
                    channel.sendToQueue(
                        "from_worker", 
                        Buffer.from(json),
                        { replyTo: msg.properties.replyTo}
                    );
                }
            }
        );

    } catch (err) {
        console.log(err);
    }
})();