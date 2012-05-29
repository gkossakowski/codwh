#include "distributed/communication.h"
#include <stdarg.h>

void Communication::debugPrint(const char* fmt, ...) {
  va_list ap;
  if (*stripe == -1) {
    printf("Worker[%d:NONE]: ", nei->my_node_number());
  } else {
    printf("Worker[%d:%d]: ", nei->my_node_number(), *stripe);
  }

  va_start(ap, fmt);
  vfprintf(stdout, fmt, ap);
  va_end(ap);

  printf("\n");
  fflush(stdout);
}

query::NetworkMessage* Communication::getMessage(bool blocking) {
  char *data;
  size_t data_len;
  query::NetworkMessage *message;

  if (!blocking) {
    data = nei->ReadPacketNotBlocking(&data_len);
    if (NULL == data)
      return NULL;
  } else {
    data = nei->ReadPacketBlocking(&data_len);
  }
  message = new query::NetworkMessage();
  message->ParseFromString(string(data, data_len));
  delete[] data;

  return message;
}

void Communication::parseMessage(query::NetworkMessage *message, bool allow_data) {
  if (message->stripe_size() > 0) {
    query::NetworkMessage::Stripe *st;
    for (int i = 0; i < message->stripe_size(); i++) {
      st = new query::NetworkMessage::Stripe();
      st->GetReflection()->Swap(message->mutable_stripe(i), st);
      jobs.push(st);
    }
  } else if (message->has_data_request()) {
    requests.push(message->release_data_request());
  } else if (allow_data && message->has_data_response()) {
    responses.push(message->release_data_response());
  } else {
    debugPrint("ERROR: parseMessage(%s, %b)", message->DebugString().c_str(), allow_data);
    assert(false);
  }

  delete message;
}

void Communication::getJob() {
  /** Wait until a job occures, than store it in a jobs queue. */
  debugPrint("Awaiting for a job");
  query::NetworkMessage *message;

  while (jobs.size() == 0) {
    message = getMessage(true);
    parseMessage(message, false); // there should be no incoming data while
                                  // awaiting for a job
  }
  return ;
}

void Communication::getRequest() {
  /** Wait until a data request occures, than store it in a requests queue */
  debugPrint("Awaiting for data request");
  query::NetworkMessage *message;

  while (requests.size() == 0) {
    message = getMessage(true);
    parseMessage(message, true);
  }
  return ;
}

void Communication::getResponse() {
  debugPrint("Awaiting for data response");
  query::NetworkMessage *message;

  while (responses.size() == 0) {
    message = getMessage(true);
    parseMessage(message, true);
  }
  return ;
}


DataSourceInterface*
Communication::openSourceInterface(int fileId) {
  return nei->OpenDataSourceFile(fileId);
}

DataSinkInterface*
Communication::openSinkInterface() {
  return nei->OpenDataSink();
}
