import {KafkaClient, Producer, Consumer} from 'kafka-node'
import { v1 as uuidv1 } from 'uuid';
import Redis from 'redis'
import {promisify} from 'util'

// Initialize database connection
const redisClient = Redis.createClient({port: 6380});
redisClient.on("error", function(error) {
  console.error(error);
});
const send_command: (command: string, args?: any[]) => Promise<any> = promisify(redisClient.send_command).bind(redisClient);

// Initialize producer
const producerClient = new KafkaClient()
const producer = new Producer(producerClient);

producer.on('error', function (err) {
  console.log(err);
});

// Initialize user consumer
const userConsumerClient = new KafkaClient()
const userConsumer = new Consumer(
    userConsumerClient,
    [
      { topic: 'users'}
    ],
    {
      autoCommit: true
    }
  );

userConsumer.on('message', function (message) {
  let value;

  try {
    value = JSON.parse(message.value as string);
  } catch (e) {
    console.log("Invalid JSON Error:", message.value);
    return;
  }

  if (value.type === "requesting_user_creation") {
    console.log("Consuming", message);
    const token = uuidv1();
    const userData = {user_token: token}
    send_command('LPUSH', ["users", userData])
      .then( (value) => console.log("user saved, ", value) )
      .then(() => {
      const payload = {topic: 'users', messages: {type: "user_creation", userData}}
      producer.send([payload], function (err, data) {
        console.log("Producing user_creation:" + data);
      });
    });
  }
});

userConsumer.on('error', function (err) {
  console.log(err);
});
