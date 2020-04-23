import {
  Consumer,
  HighLevelProducer,
  KafkaClient,
  Message,
  ProduceRequest,
} from "kafka-node";

const kafkaClient = new KafkaClient();
const consumer = new Consumer(kafkaClient, [{ topic: "create_user" }], {
  autoCommit: false,
});
const producer = new HighLevelProducer(kafkaClient);

consumer.on("message", (message: Message) => {
  if (typeof message.value !== "string") {
    throw new Error("Message type should be 'string'");
  }
  console.log(message);
  try {
    switch (message.topic) {
      case "create_user":
        return createUser(JSON.parse(message.value));
      default:
        console.error(`Unexpected topic: ${message.topic}`);
    }
  } catch (e) {
    console.error(e);
  }
});

function sendPayloads(payloads: ProduceRequest[]): void {
  producer.send(payloads, (err, data) => {
    console.log(err ? err : data);
  });
}

let current_id = 0;

function createUser(message: { username: string }) {
  sendPayloads([
    {
      topic: "user_created",
      messages: JSON.stringify({
        user_id: current_id++,
        username: message.username,
      }),
    },
  ]);
}
