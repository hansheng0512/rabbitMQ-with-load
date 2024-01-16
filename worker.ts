const amqp = require('amqplib');

async function main() {
    try {
        const connection = await amqp.connect('amqp://localhost:5672');
        const channelPDF = await connection.createChannel();
        const channelGeneral = await connection.createChannel();

        // prefetch, only allow 1 task to process by worker in PDF queue
        channelPDF.prefetch(1);

        await channelPDF.assertQueue('generate_pdf', {durable: true});
        await channelGeneral.assertQueue('general_task', {durable: true});

        console.log('generate_pdf queue is ready');
        console.log('general_task queue is ready');

        channelPDF.consume('generate_pdf', async (msg: any) => {
            const message = msg.content.toString();
            // Simulate heavy task execution
            const countdownInterval = setInterval(() => {
                const countdown = 5 - Math.floor((Date.now() - startTime) / 1000);
                // console.log(`Processing heavy task: ${message} - Time left: ${countdown}s`);
            }, 1000);
            const startTime = Date.now();
            console.log(`Heavy task received: ${message}`);
            setTimeout(() => {
                clearInterval(countdownInterval);
                console.log(`Heavy task completed: ${message}`);
                channelPDF.ack(msg!);
            }, 120000); // Adjust time as needed
        }, { noAck: false });

        channelGeneral.consume('general_task', async (msg: any) => {
            const message = msg.content.toString();
            console.log(`General task received: ${message}`);
            setTimeout(() => {
                console.log(`General task completed: ${message}`);
                channelPDF.ack(msg!);
            }, 5000); // Adjust time as needed
        }, { noAck: false });

        console.log("Queues is up now, waiting")
    } catch (e) {
        console.error(e)
    }
}

main();
