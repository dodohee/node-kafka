// Wrap a nicer JavaScript API that wraps the direct MessagePack bindings.

var buffer = require('buffer');
var events = require('events');
var mpBindings;
kafkaBindings = require(__dirname + "/../build/Release/rdkafkaBinding");


var connect = kafkaBindings.connect;
var produce = kafkaBindings.produce;
var consume = kafkaBindings.consume;
var cleanup = kafkaBindings.cleanup;

function Producer(config) {
  this.partition = config.partition || 0;
  this.brokers = config.brokers || "localhost:9092";
  this.topic = config.topic;
}

// TODO: should this be connectSync since this is sync?
Producer.prototype.connect = function(cb) {
  this._rk = connect(this.brokers, this.topic);
  cb()
}

Producer.prototype.send = function(message, partition) {
  produce(this._rk, message, partition || this.partition);
}


Producer.prototype.cleanup = function() {
  cleanup(this._rk);
}

// exports.connect = connect;
// exports.produce = produce;
// exports.consume = consume;
exports.Producer = Producer;