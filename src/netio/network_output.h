/*
 * NetworkOutput.h
 *
 *  Created on: 25-04-2012
 *      Author: ptab
 */

#ifndef NETWORKOUTPUT_H_
#define NETWORKOUTPUT_H_

#include <string>
#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>

// The class is thread safe.
class NetworkOutput {
 public:
  NetworkOutput(const std::string& host, const std::string& service);

  virtual bool SendPacket(const char* data, std::size_t  data_len);

  virtual ~NetworkOutput();
 protected:
  bool EnsureConnectionExists();

 private:
  const std::string host_;
  const std::string service_;
  boost::mutex mutex_;
  boost::asio::io_service io_service_;
  boost::asio::ip::tcp::socket socket_;

};

#endif /* NETWORKOUTPUT_H_ */
