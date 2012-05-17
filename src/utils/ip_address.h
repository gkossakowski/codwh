/*
 * ip_address.h
 * Copied from: http://stackoverflow.com/questions/2958747/what-is-the-nicest-way-to-parse-this-in-c

 */

#ifndef IP_ADDRESS_H_
#define IP_ADDRESS_H_

#include <string>

class IpAddress {
 public:
    enum Type
    {
        Unknown,
        IpV4,
        IpV6
    };
    ~IpAddress(void);

    /**
     * \brief   Gets the host address part of the IP address.
     * \author  Abi
     * \date    02/06/2010
     * \return  The host address part of the IP address.
    **/
    const std::string& getHostAddress() const;

    /**
     * \brief   Gets the port number part of the address if any.
     * \author  Abi
     * \date    02/06/2010
     * \return  The port number.
    **/
    unsigned short getPortNumber() const;

    const std::string getService() const;

    /**
     * \brief   Gets the type of the IP address.
     * \author  Abi
     * \date    02/06/2010
     * \return  The type.
    **/
    IpAddress::Type getType() const;

    /**
     * \fn  static IpAddress Parse(const std::string& ip_address_str)
     *
     * \brief   Parses a given string to an IP address.
     * \author  Abi
     * \date    02/06/2010
     * \param   ip_address_str  The ip address string to be parsed.
     * \return  Returns the parsed IP address. If the IP address is
     *          invalid then the IpAddress instance returned will have its
     *          type set to IpAddress::Unknown
    **/
    static IpAddress Parse(const std::string& ip_address_str);

    /**
     * \brief   Converts the given type to string.
     * \author  Abi
     * \date    02/06/2010
     * \param   address_type    Type of the address to be converted to string.
     * \return  String form of the given address type.
    **/
    static std::string TypeToString(IpAddress::Type address_type);
private:
    IpAddress(void);

    Type m_type;
    std::string m_hostAddress;
    unsigned short m_portNumber;
};

#endif /* IP_ADDRESS_H_ */
