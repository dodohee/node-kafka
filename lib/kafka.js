var events = require('events');
var kafkaBindings;
if (require('fs').existsSync(__dirname + '/../build')) {
  // normal situation
  kafkaBindings = require(__dirname + "/../build/Release/rdkafkaBinding");
} else {
  // the build folder was never created. Default to the precompiled build for cloudfoundry v1.
  kafkaBindings = require(__dirname + "/../_build/Release/rdkafkaBinding");
}

var connect = kafkaBindings.connect;
var produce = kafkaBindings.produce;
var consume = kafkaBindings.consume;
var cleanup = kafkaBindings.cleanup;
var setDebug = kafkaBindings.setDebug;

function Producer(config) {
  this.partition = config.partition || 0;
  this.brokers = config.brokers || "localhost:9092";
  this.topic = config.topic;
}

Producer.prototype.connect = function(cb) {
  var self = this;
  connect(this.brokers, this.topic, function(err, rk) {
    self._rk = rk;
    cb();
  });
};

Producer.prototype.send = function(message, partition, cb) {
  var self = this;
  var emitter;
  if (self._rk) {
    emitter = new events.EventEmitter();
    if (cb) {
      emitter.on("sent", cb);
    }
    if (typeof message === "object") {
      message = JSON.stringify(message);
    }
    produce(this._rk, message, partition || this.partition, emitter, function(err) {
      if (err) {
        emitter.emit("error", err);  // if error, also emit 'error'
      }
      emitter.emit("sent", err);
    });
    return emitter;
  } else {
    throw(new Error("not connected yet"));
  }
};

exports.Producer = Producer;
exports.setDebug = setDebug;

setDebug(true);