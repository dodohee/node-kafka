var kafka = require("./lib/kafka");

var producer = new kafka.Producer({
  brokers: "localhost:9092",
  partition: 0,
  topic: "test"
});


producer.connect(function() {
  var req1 = producer.send('message bytes 1', 0, function(err) {
    // same as on("sent")
  });
  req1.on("sent", function(err) {
    
  });
  req1.on("delivery", function(err) {
    
  });
  req1.on("error", function(err) {
    
  });
  
  
  
  producer.send('message bytes 2', 0).
    on("sent", function(err) {
      
    }).
    on("delivery", function(err) {
      
    }).
    on("error", function(err) {
      
    });
  producer.send('message bytes 3', 0);
  producer.send('message bytes 4', 0);
  console.log("calling cleanup");
  producer.cleanup(function() {
    // cleaned up
  });
})
