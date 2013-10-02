var kafka = require("./lib/kafka");

var producer = new kafka.Producer({
  brokers: "localhost:9092",
  partition: 0,
  topic: "test"
});


producer.connect(function() {
  producer.send('message bytes 1', 0);
  producer.send('message bytes 2', 0);
  producer.send('message bytes 3', 0);
  producer.send('message bytes 4', 0);
})
