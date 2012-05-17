/*
 * NetworkOutput.cpp
 *
 *  Created on: 25-04-2012
 *      Author: ptab
 */

#include "network_output.h"

#include <boost/asio.hpp>

#include "utils/logger.h"
#include <string.h>

using boost::asio::ip::tcp;

NetworkOutput::NetworkOutput(const std::string& host,
                             const std::string& service)
    : host_(host),
      service_(service),
      socket_(io_service_) {}

NetworkOutput::~NetworkOutput() {
}

bool NetworkOutput::EnsureConnectionExists() {
  if (!socket_.is_open()) {
    tcp::resolver resolver(io_service_);
    tcp::resolver::query query(host_, service_);
    tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
    tcp::resolver::iterator end;
    boost::system::error_code error = boost::asio::error::host_not_found;
    while (error && endpoint_iterator != end)
    {
      socket_.close();
      socket_.connect(*endpoint_iterator++, error);
    }
    if (error) {
      LOG3("%s:%s, Cannot connect: %s", host_.c_str(), service_.c_str(),
           error.message().c_str());
      return false;
    } else {
      LOG2("%s:%s, Connected.", host_.c_str(), service_.c_str());
      return true;
    }
  }
  return true;
}

bool NetworkOutput::SendPacket(const char* data, std::size_t data_len) {
  mutex_.lock();
  try {
    if (!EnsureConnectionExists()) return false;
    if (socket_.is_open()) {
      uint32_t net_buffer_length = htonl(data_len);
      CHECK(boost::asio::write(socket_,
                               boost::asio::buffer(&net_buffer_length,
                                                   sizeof(net_buffer_length)))
            == sizeof(net_buffer_length),
            "Write failed");
      CHECK(boost::asio::write(socket_,
                               boost::asio::buffer(data, data_len)) == data_len,
            "Write failed");
      // LOG3("%s:%s Sent packet of length: %ld", host_.c_str(), service_.c_str(), data_len);
      mutex_.unlock();
      return true;
    } else {
      mutex_.unlock();
      LOG2("%s:%s Cannot connect", host_.c_str(), service_.c_str());
      return false;
    };
  } catch(...) {
    LOG2("%s:%s Exception.", host_.c_str(), service_.c_str());
  }
  return false;
}
