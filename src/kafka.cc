#include <v8.h>
#include <node.h>
#include <node_buffer.h>
#include <vector>
#include <string>
#include <syslog.h>

extern "C" {
  #include <rdkafka.h>
}

using namespace v8;
using namespace node;

rd_kafka_conf_t conf;
rd_kafka_topic_conf_t topic_conf;
char errstr[512];

static void msg_delivered(rd_kafka_t *rk, void *payload, size_t len,
         rd_kafka_resp_err_t error_code, void *opaque, void *msg_opaque) {
  if (error_code) {
    printf("%% Message delivery failed: %s\n", rd_kafka_err2str(rk, error_code));
  } else {
    printf("%% Message delivered (%zd bytes)\n", len);
  }
}

static rd_kafka_t* _rk;
static rd_kafka_topic_t* _rkt;

static void logger (const rd_kafka_t *rk, int level, const char *fac, const char *buf) {
  fprintf(stderr, "RDKAFKA-%i-%s: %s: %s\n",  level, fac, rd_kafka_name(rk), buf);
}


// brokers, partition, topic
static Handle<Value>
connect(const Arguments &args) {
  HandleScope scope;
  
  v8::String::Utf8Value param0(args[0]->ToString());
  std::string brokers = std::string(*param0);
    
  v8::String::Utf8Value param1(args[1]->ToString());
  std::string topic = std::string(*param1);

  printf("brokers, %ld, %s\n", brokers.length(), brokers.c_str());
  printf("topic, %ld, %s\n", topic.length(), topic.c_str());
  
  rd_kafka_t* rk;

  if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, &conf, errstr, sizeof(errstr)))) {
    fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
    exit(1);
  }


  rd_kafka_set_logger(rk, logger);
  rd_kafka_set_log_level(rk, LOG_DEBUG);

  // set broker
  if (rd_kafka_brokers_add(rk, brokers.c_str()) == 0) {
    fprintf(stderr, "%% No valid brokers specified\n");
    exit(1);
  }

  /* Create topic */
  rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk, topic.c_str(), &topic_conf);
  if (!rkt) {
    fprintf(stderr, "%% can't create topic\n");
    exit(1);
  }
  
  _rkt = rkt;
  
  printf("rk1, %lld %p\n", sizeof(rk), rk);
  printf("rkt1, %lld %p\n", sizeof(rkt), rkt);


  Local<ObjectTemplate> rk_template = ObjectTemplate::New();
  rk_template->SetInternalFieldCount(2);
  // Persistent<Context> context = Context::New(NULL, global_template);



  Local<Object> obj = rk_template->NewInstance();
  printf("setting internal 0\n");
  obj->SetInternalField(0, v8::External::New((void *)rk));
  printf("setting internal 1\n");
  obj->SetInternalField(1, v8::External::New((void *)rkt));
  obj->Set(String::New("intern"), Number::New(5));
  printf("internal set\n");
  
  
  
  printf("rk, %lld %p\n", sizeof(v8::External::Cast(*(obj->GetInternalField(0)))->Value()), v8::External::Cast(*(obj->GetInternalField(0)))->Value());
  printf("rkt, %lld %p\n", sizeof(v8::External::Cast(*(obj->GetInternalField(1)))->Value()), v8::External::Cast(*(obj->GetInternalField(1)))->Value());
  
  
  // rd_kafka_produce(rkt, 0, RD_KAFKA_MSG_F_COPY, "hello world", 11, NULL, 0, NULL);
  // rd_kafka_poll(rk, 10);
  
  // printf("waiting\n");
  // while (rd_kafka_poll(rk, 1000) != -1)
  //   continue;
  // printf("done\n");
  
  
  return scope.Close(obj);
}


static Handle<Value>
consume(const Arguments &args) {
  HandleScope scope;
  return scope.Close(Undefined());
}

static Handle<Value>
cleanup(const Arguments &args) {
  Local<Object> obj = args[0]->ToObject();
  rd_kafka_t* rk = static_cast<rd_kafka_t*>(v8::External::Cast(*(obj->GetInternalField(0)))->Value());

  printf("waiting\n");
  while (rd_kafka_poll(rk, 1000) != -1)
    continue;
  printf("done\n");
}

static Handle<Value>
produce(const Arguments &args) {
  HandleScope scope;
  
  Local<Object> obj = args[0]->ToObject();
  
  v8::String::Utf8Value param1(args[1]->ToString());
  std::string message = std::string(*param1);
  
  uint64_t partition = static_cast<uint64_t>(args[2]->NumberValue());
  
  Handle<Number> number = Handle<Number>::Cast(obj->Get(String::New("intern")));
  uint64_t value = static_cast<uint64_t>(number->NumberValue());
  
  
  printf("value %lld\n", value);
  
  rd_kafka_t* rk = (rd_kafka_t*)(v8::External::Cast(*(obj->GetInternalField(0)))->Value());
  rd_kafka_topic_t* rkt = (rd_kafka_topic_t*)(v8::External::Cast(*(obj->GetInternalField(1)))->Value());
  
  if (rk != _rk) {
    printf("wrong rk\n");
  }
  if (rkt != _rkt) {
    printf("wrong rkt\n");
  }
  
  
  printf("rk, %p\n", rk);
  printf("rkt, %p\n", rkt);
  printf("partition %lld\n", partition);
  printf("message, %ld, %s\n", message.length(), message.c_str());
  
  
  rd_kafka_produce(rkt, (int32_t)partition, RD_KAFKA_MSG_F_COPY, (char*)(message.c_str()), (size_t)(message.length()), NULL, 0, NULL);
  // rd_kafka_produce(rkt, 0, RD_KAFKA_MSG_F_COPY, "hello world 2", 11, NULL, 0, NULL);
  rd_kafka_poll(rk, 10);
  
  printf("sent\n");
  
  return scope.Close(Undefined());
}

extern "C" void init(Handle<Object> target) {
  HandleScope scope;
  
  printf("kafka-init\n");
  
  // Kafka configuration Base configuration on the default config.
  rd_kafka_defaultconf_set(&conf);
  
  // Set up a message delivery report callback.
  // It will be called once for each message, either on succesful delivery to broker,
  // or upon failure to deliver to broker.
  conf.producer.dr_cb = msg_delivered;
  
  if (rd_kafka_conf_set(&conf, "debug", "all", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    printf("%% Debug configuration failed: %s\n", errstr);
    exit(1);
  }
  
  // Topic configuration
  // Base topic configuration on the default topic config.
  rd_kafka_topic_defaultconf_set(&topic_conf);
  
  NODE_SET_METHOD(target, "produce", produce);
  NODE_SET_METHOD(target, "consume", consume);
  NODE_SET_METHOD(target, "connect", connect);
  NODE_SET_METHOD(target, "cleanup", cleanup);
}

NODE_MODULE(rdkafkaBinding, init);
