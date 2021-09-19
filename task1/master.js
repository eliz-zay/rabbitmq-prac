amqp = require('amqplib');
const { spawn } = require('child_process');

function initWorkers(workerNum, cities) {
    for (let i = 0; i < workerNum; i++) {
        const child = spawn(process.argv[0], ['worker.js', cities[i]]);
    
        child.stdout.on('data', (data) => {
            console.log(`stdout ${i}:\n${data}`);
        });
    
        child.on('error', (error) => {
            console.error(`error ${i}: ${error.message}`);
        });
          
        child.on('close', (code) => {
            console.log(`worker ${i} exited with code ${code}`);
        });
    }
}

(async () => {
    try {
        const args = process.argv.slice(2);
        const workerNum = args[0];

        const cityFiles = [
            'data/nyc.xml', 
            'data/philadelphia.xml',
            'data/lasvegas.xml'
        ];

        initWorkers(workerNum, cityFiles);

        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await channel.assertQueue('rpc_queue', { durable: true });
        await channel.assertQueue('from_worker', { durable: false });
        await channel.assertExchange('to_worker', 'fanout', { durable: false });

        const clientRoads = new Map();

        channel.consume(
            'from_worker', 
            workerMsg => {
                channel.ack(workerMsg);

                const clientQueue = workerMsg.properties.replyTo;
                const roads = clientRoads.get(clientQueue);

                roads.push(workerMsg.content.toString());

                if (roads.length == workerNum) {
                    const json = JSON.stringify(roads);
                    
                    channel.sendToQueue(
                        clientQueue,
                        Buffer.from(json)
                    );

                    clientRoads.delete(clientQueue);
                }

            }
        );

        channel.consume(
            'rpc_queue',
            clientMsg => {
                channel.ack(clientMsg);

                const letter = clientMsg.content.toString();
                console.log('*** Broadcasting letter: ' + letter + '   ***\n');

                clientRoads.set(clientMsg.properties.replyTo, []);

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