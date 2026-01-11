const express = require("express");
const fs = require("fs");
const amqp = require('amqplib');
const mongodb = require("mongodb");

if (!process.env.PORT) {
    throw new Error("Please specify the port number for the HTTP server with the environment variable PORT.");
}

if (!process.env.RABBIT) {
    throw new Error("Please specify the name of the RabbitMQ host using environment variable RABBIT");
}

// env variables
const PORT = process.env.PORT;
const RABBIT = process.env.RABBIT;
const DBHOST = process.env.DBHOST;
const DBNAME = process.env.DBNAME;

//
// Application entry point.
//
async function main() {

    // Create a client and connect to the mongodb database.
    const client = await mongodb.MongoClient.connect(DBHOST);
    const db = client.db(DBNAME);
    const videosCollection = db.collection("videos");

    console.log(`Connecting to RabbitMQ server at ${RABBIT}.`);

    const messagingConnection = await amqp.connect(RABBIT); // Connects to the RabbitMQ server.
    
    console.log("Connected to RabbitMQ.");

    const messageChannel = await messagingConnection.createChannel(); // Creates a RabbitMQ messaging channel.

	await messageChannel.assertExchange("viewed", "fanout"); // Asserts that we have a "viewed" exchange.

    //
    // Broadcasts the "viewed" message to other microservices.
    //
	function broadcastViewedMessage(messageChannel, videoPath, videoId) {
	    console.log(`Publishing message on "viewed" exchange.`);
	        
	    const msg = { videoPath: videoPath, videoId: videoId};
	    const jsonMsg = JSON.stringify(msg);
	    messageChannel.publish("viewed", "", Buffer.from(jsonMsg)); // Publishes message to the "viewed" exchange.
	}

    const app = express();

    app.get("/video", async (req, res) => { // Route for streaming video.

        // get the video id from the 'id' parameter and query the database
        const videoId = req.query.id;
        const videoRecord = await videosCollection.findOne({ _id: videoId });
        if (!videoRecord) {
            // The video was not found.
            res.sendStatus(404);
            return;
        }

        // get the video path and serve the video
        const videoPath = "./videos/" + videoRecord.videoPath;
        const stats = await fs.promises.stat(videoPath);

        res.writeHead(200, {
            "Content-Length": stats.size,
            "Content-Type": "video/mp4",
        });
    
        fs.createReadStream(videoPath).pipe(res);

        broadcastViewedMessage(messageChannel, videoPath, videoId); // Sends the "viewed" message to indicate this video has been watched.
    });

    app.listen(PORT, () => {
        console.log("Microservice online.");
    });
}

main()
    .catch(err => {
        console.error("Microservice failed to start.");
        console.error(err && err.stack || err);
    });