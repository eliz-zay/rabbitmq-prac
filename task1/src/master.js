const amqp = require('amqplib');
const { spawn } = require('child_process');

function initWorkers(workerNum, cities) {
    for (let i = 0; i < workerNum; i++) {
        const child = spawn(process.argv[0], ['src/worker.js', cities[i]]);
    
        child.stdout.on('data', (data) => {
            console.log(`Worker ${i}:\n${data}`);
        });
    
        child.on('error', (error) => {
            console.error(`Worker ${i}: ${error.message}`);
        });
          
        child.on('close', (code) => {
            console.log(`Worker ${i} exited with code ${code}`);
            workerNum--;
        });
    }
}

(async () => {
    try {
        const args = process.argv.slice(2);
        const workerNum = args[0];

        const clientRoads = new Map();
        const cityFiles = [
            'data/nyc.xml', 
            'data/philadelphia.xml',
            'data/las_vegas.xml'
        ];

        if (workerNum != parseInt(workerNum, 10)) {
            console.log(`\n${workerNum} is not an integer\n`);
            return 0;
        }
        if (workerNum > cityFiles.length) {
            console.log(`\nMax number of workers: ${cityFiles.length}\n`);
            return 0;
        }
        if (workerNum < 1) {
            console.log('\nNumber of workers cannot be below 1\n');
            return 0;
        }

        initWorkers(workerNum, cityFiles);

        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await channel.assertQueue('rpc_queue');
        await channel.assertQueue('from_worker');
        await channel.assertExchange('to_worker', 'fanout');

        channel.consume(
            'from_worker', 
            workerMsg => {
                channel.ack(workerMsg);

                const clientQueue = workerMsg.properties.replyTo;
                const roads = clientRoads.get(clientQueue);

                const jsonMsg = JSON.parse(workerMsg.content.toString());
                roads.push(jsonMsg.isError ? '' : workerMsg.content.toString())

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
                console.log(`Broadcasting letter ${letter}`);

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