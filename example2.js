var kafka = require("./lib/kafka");

var producer = new kafka.Producer({
  brokers: "localhost:9092",
  partition: 0,
  topic: "test"
});


producer.connect(function() {
  console.log("====== connected-1")
  console.log("====== producing-1");
  var req1 = producer.send('message ' + new Date().toString(), 0, function(err) {
    console.log("====== sent-1a", err);
  });
  req1.on("sent", function(err) {
    console.log("====== sent-1b", err);
  });
  req1.on("delivery", function(err, length) {
    console.log("====== delivery-1", err);
  });
  req1.on("error", function(err) {
    console.log("====== error-1", err);
  });
  
  
  console.log("====== producing-2");
  producer.send('message bytes 2', 0).
    on("sent", function(err) {
      console.log("====== sent-2", err);
    }).
    on("delivery", function(err) {
      console.log("====== delivery-2", err);
    }).
    on("error", function(err) {
      console.log("====== error-2")
    });
  console.log("====== producing-3");
  producer.send('message bytes 3', 0);
  console.log("====== producing-4");
  producer.send('message bytes 4', 0);
})
