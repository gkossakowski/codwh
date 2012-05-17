// Copied from: http://stackoverflow.com/questions/2958747/what-is-the-nicest-way-to-parse-this-in-c

#include "ip_address.h"
#include <boost/xpressive/xpressive.hpp>
#include <string>
#include <stdlib.h>

namespace bxp = boost::xpressive;

static const std::string RegExIpV4_IpFormatHost = "^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]+(\\:[0-9]{1,5})?$";
static const std::string RegExIpV4_StringHost = "^[A-Za-z0-9]+(\\:[0-9]+)?$";

IpAddress::IpAddress(void)
:m_type(Unknown)
,m_portNumber(0)
{
}

IpAddress::~IpAddress(void)
{
}

IpAddress IpAddress::Parse( const std::string& ip_address_str )
{
    IpAddress ipaddress;
    bxp::sregex ip_regex = bxp::sregex::compile(RegExIpV4_IpFormatHost);
    bxp::sregex str_regex = bxp::sregex::compile(RegExIpV4_StringHost);
    bxp::smatch match;
    if (bxp::regex_match(ip_address_str, match, ip_regex) || bxp::regex_match(ip_address_str, match, str_regex))
    {
        ipaddress.m_type = IpV4;
        // Anything before the last ':' (if any) is the host address
        std::string::size_type colon_index = ip_address_str.find_last_of(':');
        if (std::string::npos == colon_index)
        {
            ipaddress.m_portNumber = 0;
            ipaddress.m_hostAddress = ip_address_str;
        }else{
            ipaddress.m_hostAddress = ip_address_str.substr(0, colon_index);
            ipaddress.m_portNumber = atoi(ip_address_str.substr(colon_index+1).c_str());
        }
    }
    return ipaddress;
}

std::string IpAddress::TypeToString( Type address_type ) {
  switch(address_type) {
    case IpV4: return "IP Address Version 4";
    case IpV6: return "IP Address Version 6";
    default: return "Unknown";
  }
}

const std::string& IpAddress::getHostAddress() const
{
  return m_hostAddress;
}

const std::string IpAddress::getService() const {
  std::ostringstream ss;
  ss << m_portNumber;
  return ss.str();
}

unsigned short IpAddress::getPortNumber() const
{
    return m_portNumber;
}

IpAddress::Type IpAddress::getType() const
{
    return m_type;
}
