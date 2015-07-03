#include <v8.h>
#include <node.h>
#include <node_buffer.h>
#include <vector>
#include <string>
#include <syslog.h>
#include <stdarg.h>
#include <cstdlib>

extern "C" {
  #include <librdkafka/rdkafka.h>
}

using namespace v8;
using namespace node;

rd_kafka_conf_t* conf;
rd_kafka_topic_conf_t* topic_conf;
char errstr[512];

static int counter = 0;

struct ProduceBaton {
  Persistent<Function> callback;
  Persistent<Object> emitter;
  bool error;
  std::string error_message;
  std::string message;
  uint64_t partition;
  rd_kafka_t* rk;
  rd_kafka_topic_t* rkt;
  size_t delivered_length;
  uv_async_t async;
  int id;
};

static bool debug_state = false;

static Handle<Value>
setDebug(const Arguments &args) {
  HandleScope scope;
  
  debug_state = args[0]->BooleanValue();
  
  return scope.Close(Undefined());
}
  
static void debug(const char* format, ...) {
  va_list args;
  va_start(args, format);
  if (debug_state) {
    time_t rawtime;
    struct tm* timeinfo;
    char timestamp[80];

    time(&rawtime);
    timeinfo = localtime(&rawtime);
    strftime(timestamp, 80, "kafka - %D %H:%M:%S - ", timeinfo);
    printf(timestamp);
    vprintf(format, args);
  }
  va_end(args);
}

void produceFree(uv_handle_t* w) {
  debug("produceFree start\n");
  uv_unref(w);
  ProduceBaton* baton = (ProduceBaton*)w->data;
  baton->id = -1;
  baton->callback.Dispose();
  baton->emitter.Dispose();
  delete baton;
  debug("produceFree end\n");
}

static void produceAsyncDelivery(uv_async_t* w, int revents) {
  HandleScope scope;
  debug("produceAsyncDelivery start\n");
  
  ProduceBaton* baton = (ProduceBaton*)w->data;
  
  if (baton) {
    debug("produceAsyncDelivery with baton %d\n", baton->id);
  } else {
    debug("produceAsyncDelivery without baton\n");
  }
  
  if (baton && baton->emitter->IsObject()) {
    debug("emitter is an object\n");
    Local<Function> emit = Local<Function>::Cast(baton->emitter->Get(String::New("emit"))); 
    debug("emit is a method %d\n", emit->IsFunction());
    if (baton->error) {
      Local<Value> argv[] = {
        Local<Value>::New(String::New("error")),
        Exception::Error(String::New(baton->error_message.c_str()))
      };
      emit->Call(baton->emitter, 2, argv);
    } else {
      Local<Value> argv[] = {
        Local<Value>::New(String::New("delivery")),
        Local<Value>::New(Integer::New(baton->delivered_length))
      };
      emit->Call(baton->emitter, 2, argv);
    }
  }
  if (baton) {
    debug("cleanup produce baton %d\n", baton->id);
    uv_close((uv_handle_t*)&baton->async, produceFree);
  }
  debug("produceAsyncDelivery end\n");
}


static void msg_delivered(rd_kafka_t* rk, void* payload, size_t len, rd_kafka_resp_err_t error_code, void* opaque, void* msg_opaque) {
  ProduceBaton* baton = (ProduceBaton*)msg_opaque;
  if (baton) {
    debug("msg_delivered with baton %d\n", baton->id);
  } else {
    debug("msg_delivered without baton\n");
  }
  if (error_code) {
    debug("%% Message delivery failed: %s\n", rd_kafka_err2str(rd_kafka_errno2err(error_code)));
    baton->error = true;
    baton->error_message = rd_kafka_err2str(rd_kafka_errno2err(error_code));
  } else {
    debug("%% Message delivered (%zd bytes)\n", len);
    baton->delivered_length = len;
  }
  baton->async.data = baton;
  uv_async_send(&baton->async);
}

static void logger(const rd_kafka_t *rk, int level, const char *fac, const char *buf) {
  debug("%i-%s: %s: %s\n",  level, fac, rd_kafka_name(rk), buf);
}

struct PollBaton {
  rd_kafka_t* rk;
  uv_async_t async;
};

void poll_events(void* arg) {
  PollBaton* baton = static_cast<PollBaton*>(arg);
  
  debug("poll_events start\n");
  int nbev;
  while ((nbev = rd_kafka_poll(baton->rk, 5000)) != -1) {
    // debug("polling returned %d, respinning\n", nbev);
  }
  baton->async.data = baton;
  uv_async_send(&baton->async);
  debug("poll_events end\n");
}

struct ConnectBaton {
  Persistent<Function> callback;
  bool error;
  std::string error_message;
  rd_kafka_t* rk;
  rd_kafka_topic_t* rkt;
  std::string brokers;
  std::string topic;
  uv_async_t async;
};

void ConnectAsyncWork(uv_work_t* req) {
  // no v8 access here!
  debug("ConnectAsyncWork start\n");
  ConnectBaton* baton = static_cast<ConnectBaton*>(req->data);

  if (!(baton->rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)))) {
    fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
    exit(1);
  }

  
  PollBaton* poll_baton = new PollBaton();
  poll_baton->rk = baton->rk;
  poll_baton->async = baton->async;
  uv_thread_t poll_events_id;
  uv_thread_create(&poll_events_id, poll_events, poll_baton);
  
  
  rd_kafka_set_logger(baton->rk, logger);
  // rd_kafka_set_log_level(baton->rk, LOG_DEBUG);

  // set broker
  if (rd_kafka_brokers_add(baton->rk, baton->brokers.c_str()) == 0) {
    fprintf(stderr, "%% No valid brokers specified\n");
    exit(1);
  }

  /* Create topic */
  baton->rkt = rd_kafka_topic_new(baton->rk, baton->topic.c_str(), topic_conf);
  if (!baton->rkt) {
    fprintf(stderr, "%% can't create topic\n");
    exit(1);
  }
    
  debug("rk1, %lu %p\n", sizeof(baton->rk), baton->rk);
  debug("rkt1, %lu %p\n", sizeof(baton->rkt), baton->rkt);


  debug("ConnectAsyncWork end\n");
}


void connectFree(uv_handle_t* w) {
  debug("connectFree start\n");
  ConnectBaton* baton = (ConnectBaton*)w->data;
  baton->callback.Dispose();
  delete baton;
  uv_unref(w);
  debug("connectFree end\n");
}


void ConnectAsyncAfter(uv_work_t* req) {
    HandleScope scope;
    ConnectBaton* baton = static_cast<ConnectBaton*>(req->data);
    Persistent<Function> callback = baton->callback;

    debug("ConnectAsyncAfter start\n");
    if (baton->error) {
      debug("error detected");
      Local<Value> err = Exception::Error(String::New(baton->error_message.c_str()));
      Local<Value> argv[] = { err };
      callback->Call(Context::GetCurrent()->Global(), 1, argv);
    } else {
      Local<ObjectTemplate> rk_template = ObjectTemplate::New();
      rk_template->SetInternalFieldCount(2);

      Local<Object> obj = rk_template->NewInstance();
      debug("setting internal 0\n");
      obj->SetInternalField(0, v8::External::New((void *)baton->rk));
      debug("setting internal 1\n");
      obj->SetInternalField(1, v8::External::New((void *)baton->rkt));
      obj->Set(String::New("intern"), Number::New(5));  // TODO: remove

      debug("rk, %lu %p\n", sizeof(v8::External::Cast(*(obj->GetInternalField(0)))->Value()), v8::External::Cast(*(obj->GetInternalField(0)))->Value());
      debug("rkt, %lu %p\n", sizeof(v8::External::Cast(*(obj->GetInternalField(1)))->Value()), v8::External::Cast(*(obj->GetInternalField(1)))->Value());
      
      Local<Value> argv[] = {
        Local<Value>::New(Null()),
        obj
      };
      callback->Call(Context::GetCurrent()->Global(), 2, argv);
    }

    baton->async.data = baton;
    uv_close((uv_handle_t*)&baton->async, connectFree);
    debug("ConnectAsyncAfter end\n");
}


// this can never be called because uv_close has been called
void connectAsyncCallback(uv_async_t* w, int revents) {
  debug("connectAsyncCallback start\n");
  uv_unref((uv_handle_t*)w);
  PollBaton* baton = (PollBaton*)w->data;
  w->data = 0;
  if (baton) {
    delete baton;
  } else {
    debug("connectAsyncCallback with no baton");
  }
  debug("connectAsyncCallback end\n");
}


// brokers, partition, topic
static Handle<Value>
connect(const Arguments &args) {
  HandleScope scope;
  
  ConnectBaton* baton = new ConnectBaton();
  
  v8::String::Utf8Value param0(args[0]->ToString());
  baton->brokers = std::string(*param0);
    
  v8::String::Utf8Value param1(args[1]->ToString());
  baton->topic = std::string(*param1);

  Local<Function> callback = Local<Function>::Cast(args[2]);
  baton->callback = Persistent<Function>::New(callback);
  
  baton->error = false;
  
  debug("brokers, %ld, %s\n", baton->brokers.length(), baton->brokers.c_str());
  debug("topic, %ld, %s\n", baton->topic.length(), baton->topic.c_str());
  
  uv_async_init(uv_default_loop(), &baton->async, connectAsyncCallback);
  uv_ref((uv_handle_t*)&baton->async);
  
  uv_work_t* req = new uv_work_t();
  req->data = baton;
  int status = uv_queue_work(uv_default_loop(), req, ConnectAsyncWork, (uv_after_work_cb)ConnectAsyncAfter);
  
  if (status != 0) {
    fprintf(stderr, "connect: couldn't queue work");
    debug("connect: couldn't queue work\n");
    Local<Value> err = Exception::Error(String::New("Could not queue work"));
    Local<Value> argv[] = { err };
    baton->callback->Call(Context::GetCurrent()->Global(), 1, argv);
    uv_close((uv_handle_t*)&baton->async, connectFree);
  }  
  
  return scope.Close(Undefined());
}


static Handle<Value>
consume(const Arguments &args) {
  HandleScope scope;
  return scope.Close(Undefined());
}

static Handle<Value>
cleanup(const Arguments &args) {
  HandleScope scope;
  Local<Object> obj = args[0]->ToObject();
  rd_kafka_t* rk = static_cast<rd_kafka_t*>(v8::External::Cast(*(obj->GetInternalField(0)))->Value());

  debug("waiting\n");
  while (rd_kafka_poll(rk, 1000) != -1)
    continue;
  debug("done\n");
  return scope.Close(Undefined());
}


void ProduceAsyncWork(uv_work_t* req) {
  // no v8 access here!
  ProduceBaton* baton = static_cast<ProduceBaton*>(req->data);
  debug("ProduceAsyncWork start %d\n", baton->id);
  
  int status = rd_kafka_produce(baton->rkt, (int32_t)baton->partition, RD_KAFKA_MSG_F_COPY, (char*)(baton->message.c_str()), (size_t)(baton->message.length()), NULL, 0, baton);
  
  if (status != 0) {
    baton->error = true;
    baton->error_message = "produce: maximum number of outstanding messages exceeded";
  }
  
  debug("sent %d\n", status);
  debug("ProduceAsyncWork end\n");
}

void ProduceAsyncAfter(uv_work_t* req) {
  HandleScope scope;
  ProduceBaton* baton = static_cast<ProduceBaton*>(req->data);
  Persistent<Function> callback = baton->callback;
  debug("ProduceAsyncAfter start %d\n", baton->id);
  
  if (baton->error) {
    debug("error detected");
    Local<Value> err = Exception::Error(String::New(baton->error_message.c_str()));
    Local<Value> argv[] = { err };
    callback->Call(Context::GetCurrent()->Global(), 1, argv);
  } else {
    Local<Value> argv[] = {
      Local<Value>::New(Null())
    };
    callback->Call(Context::GetCurrent()->Global(), 1, argv);
  }
  
  debug("ProduceAsyncAfter end\n");
}

static Handle<Value>
produce(const Arguments &args) {
  HandleScope scope;
  
  ProduceBaton* baton = new ProduceBaton();
  baton->id = counter++;
  
  Local<Object> obj = args[0]->ToObject();
  
  v8::String::Utf8Value param1(args[1]->ToString());
  baton->message = std::string(*param1);
  
  baton->partition = static_cast<uint64_t>(args[2]->NumberValue());
  
  Handle<Number> number = Handle<Number>::Cast(obj->Get(String::New("intern")));
  uint64_t value = static_cast<uint64_t>(number->NumberValue());

  Local<Object> emitter = Local<Object>::Cast(args[3]);
  baton->emitter = Persistent<Object>::New(emitter);

  Local<Function> callback = Local<Function>::Cast(args[4]);
  baton->callback = Persistent<Function>::New(callback);
  
  baton->error = false;
  
  debug("value %lld\n", value);
  
  baton->rk = (rd_kafka_t*)(v8::External::Cast(*(obj->GetInternalField(0)))->Value());
  baton->rkt = (rd_kafka_topic_t*)(v8::External::Cast(*(obj->GetInternalField(1)))->Value());  
  
  debug("rk, %p\n", baton->rk);
  debug("rkt, %p\n", baton->rkt);
  debug("partition %lld\n", baton->partition);
  debug("message, %ld, %s\n", baton->message.length(), baton->message.c_str());
  
  
  uv_async_init(uv_default_loop(), &baton->async, produceAsyncDelivery);
  uv_ref((uv_handle_t*)&baton->async);
  
  
  uv_work_t* req = new uv_work_t();
  req->data = baton;
  int status = uv_queue_work(uv_default_loop(), req, ProduceAsyncWork, (uv_after_work_cb)ProduceAsyncAfter);
  
  debug("produce uv_queue_work %d\n", status);
  if (status != 0) {
    fprintf(stderr, "produce uv_queue_work error\n");
    debug("produce: couldn't queue work");
    Local<Value> err = Exception::Error(String::New("Could not queue work for produce"));
    Local<Value> argv[] = { err };
    baton->callback->Call(Context::GetCurrent()->Global(), 1, argv);
    uv_close((uv_handle_t*)&baton->async, produceFree);
  }
  return scope.Close(Undefined());
}

extern "C" void init(Handle<Object> target) {
  HandleScope scope;
  
  // Kafka configuration Base configuration on the default config.
  conf = rd_kafka_conf_new();
  
  // Set up a message delivery report callback.
  // It will be called once for each message, either on succesful delivery to broker,
  // or upon failure to deliver to broker.
  rd_kafka_conf_set_dr_cb(conf, msg_delivered);
  
  if (rd_kafka_conf_set(conf, "debug", "all", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    debug("%% Debug configuration failed: %s\n", errstr);
    exit(1);
  }
  
  // Topic configuration
  // Base topic configuration on the default topic config.
  topic_conf = rd_kafka_topic_conf_new();
  
  NODE_SET_METHOD(target, "produce", produce);
  NODE_SET_METHOD(target, "consume", consume);
  NODE_SET_METHOD(target, "connect", connect);
  NODE_SET_METHOD(target, "cleanup", cleanup);
  NODE_SET_METHOD(target, "setDebug", setDebug);  
}

NODE_MODULE(rdkafkaBinding, init);
