const amqp = require('amqplib');

async function pushHeavyTask() {
    try {
        const connection = await amqp.connect('amqp://localhost:5672');
        const channelPDF = await connection.createChannel();

        await channelPDF.assertQueue('generate_pdf', { durable: true });

        const tasks = ['task1', 'task2', 'task3']; // Replace with your tasks

        tasks.forEach((task) => {
            channelPDF.sendToQueue('generate_pdf', Buffer.from(task), { persistent: true });
            console.log(`Task enqueued: ${task}`);
        });

        setTimeout(() => {
            connection.close();
            console.log('END')
            process.exit(0);
        }, 5000); // Allow time to send tasks before exiting

    } catch (e) {
        console.error(e)
    }
}

async function pushGeneralTask() {
    try {
        const connection = await amqp.connect('amqp://localhost:5672');
        const channelGeneral = await connection.createChannel();

        await channelGeneral.assertQueue('general_task', { durable: true });

        const tasks = ['task1', 'task2', 'task3']; // Replace with your tasks

        tasks.forEach((task) => {
            channelGeneral.sendToQueue('general_task', Buffer.from(task), { persistent: true });
            console.log(`Task enqueued: ${task}`);
        });

        setTimeout(() => {
            connection.close();
            console.log('END')
            process.exit(0);
        }, 5000); // Allow time to send tasks before exiting

    } catch (e) {
        console.error(e)
    }
}

pushHeavyTask();
pushGeneralTask();
