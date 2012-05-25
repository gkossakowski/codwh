/*
 * network_input.cc
 *
 *  Created on: 25-04-2012
 *      Author: ptab
 */

#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/thread/thread.hpp>
#include <string.h>

#include "network_input.h"
#include "utils/logger.h"
#include "utils/pcqueue.h"

using boost::asio::ip::tcp;

InputConnection::InputConnection(boost::asio::io_service* io_service,
                                 boost::asio::ip::tcp::socket* socket,
                                 boost::asio::ip::tcp::endpoint* endpoint,
                                 util::PCQueue<Packet*>* pc_queue)
    : status_(WAIT_FOR_LENGTH),
      io_service_(io_service),
      socket_(socket),
      endpoint_(endpoint),
      pc_queue_(pc_queue) {
  CHECK(pc_queue_ != NULL, "Illegal state.");
  schedule_read();
};

void InputConnection::schedule_read() {
  switch (status_) {
    case WAIT_FOR_LENGTH:
      net_buffer_length_ = 0;
      boost::asio::async_read(*socket_,
          boost::asio::buffer(&net_buffer_length_, sizeof(net_buffer_length_)),
          boost::bind(&InputConnection::read_handle, this,
                      boost::asio::placeholders::error,
                      boost::asio::placeholders::bytes_transferred));
      break;
    case WAIT_FOR_DATA:
      buffer_content_.reset(new Packet(buffer_length()));
      boost::asio::async_read(*socket_,
          boost::asio::buffer(buffer_content_->data(), buffer_content_->size()),
          boost::bind(&InputConnection::read_handle, this,
                      boost::asio::placeholders::error,
                      boost::asio::placeholders::bytes_transferred));
      break;
  }
}

void InputConnection::read_handle(const boost::system::error_code& error,
                                  std::size_t bytes_transferred) {
  if (!error) {
    switch (status_) {
      case WAIT_FOR_LENGTH:
        CHECK(bytes_transferred == sizeof(net_buffer_length_), "");
        LOG2("%s Will be reading packet of %d chars", describe().c_str(), buffer_length());
        status_ = WAIT_FOR_DATA;
        break;
      case WAIT_FOR_DATA:
        CHECK(bytes_transferred == buffer_length(), "");
        //LOG2("%s Got: %s", describe().c_str(), buffer_content_->data());
        pc_queue_->Produce(buffer_content_.release());
        status_ = WAIT_FOR_LENGTH;
        break;
    }
    schedule_read();
  } else {
    LOG4("%s Error: %s %d %s\n", describe().c_str(), error.category().name(),
         error.value(), error.message().c_str());
   if (error.value() == boost::asio::error::eof) {
     LOG1("%s CONNECTION LOST", describe().c_str());
     // TODO(ptab) In a full-framework implementation we should destroy the InputConnection instance
     // here, as it is not useful anymore.
   } else {
     CHECK(false, "Unexpected error in connection layer");
   }
  }
}

std::string InputConnection::describe() const {
  std::ostringstream ss;
  ss << endpoint_->address().to_string() << ":" << endpoint_->port();
  return ss.str();
}

// --------------- InputConnection --------------------------------------------

NetworkInput::NetworkInput(short listening_port)
    : acceptor_(io_service_, tcp::endpoint(tcp::v4(), listening_port), true),
      pc_queue_(new util::PCQueue<Packet*>(/*max_size = */ kQueueSize)) {
  start_accept();
  for (std::size_t i = 0; i < kThreadsCount; ++i) {
    boost::shared_ptr<boost::thread> thread(
        new boost::thread(boost::bind((size_t (boost::asio::io_service::*)())(&boost::asio::io_service::run), &io_service_)));
    threads_.push_back(thread);
  }
}


void NetworkInput::start_accept() {
  waiting_endpoint_.reset(new boost::asio::ip::tcp::endpoint());
  waiting_socket_.reset(new boost::asio::ip::tcp::socket(io_service_));
  acceptor_.async_accept(*waiting_socket_, *waiting_endpoint_,
      boost::bind(&NetworkInput::accept_connection, this,
              boost::asio::placeholders::error));

}


void NetworkInput::accept_connection(const boost::system::error_code& e) {
  LOG2("%s:%d CONNECTED.", waiting_endpoint_->address().to_string().c_str(),
                           waiting_endpoint_->port());
  connections_.push_back(new InputConnection(&io_service_,
                                             waiting_socket_.release(),
                                             waiting_endpoint_.release(),
                                             pc_queue_.get()));
  start_accept();
}

NetworkInput::~NetworkInput() {
  io_service_.stop();
  pc_queue_->StopWaiters();
  for (std::size_t i = 0; i < threads_.size(); ++i) {
     threads_[i]->join();
  }
}

char* NetworkInput::ReadPacketBlocking(std::size_t* data_len) {
    char * res;
    {
      boost::scoped_ptr<Packet> packet;
      {
        Packet* packet_ptr;
        pc_queue_->Consume(packet_ptr);
        packet.reset(packet_ptr);
      }
      *data_len = packet->size();
      res =  packet->release_data();
      printf("After release data");
      assert(packet->data_ == NULL);
    }
      printf("After scoped_ptr destruction");
  return res;
}

char* NetworkInput::ReadPacketNotBlocking(std::size_t* data_len) {
  boost::scoped_ptr<Packet> packet;
  {
    Packet* packet_ptr;
    if (!pc_queue_->TryConsume(packet_ptr)) return NULL;
    packet.reset(packet_ptr);
  }
  *data_len = packet->size();
  return packet->release_data();
}


