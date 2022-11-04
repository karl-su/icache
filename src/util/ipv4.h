#ifndef __IP_V4_H__
#define __IP_V4_H__

#include <stdint.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string>

namespace ipv4 {

typedef union NAddr {
    uint64_t n;
    struct {
        uint32_t ip;
        uint16_t port;
    } addr;

    NAddr() : n(0) {}
} NAddr;

typedef struct Addr {
    std::string _ip;
    uint16_t _port;

    NAddr _naddr;

    Addr() : _ip(""), _port(0), _naddr(0) {}

    Addr(const string& ipport)
    {
        set(ipport);
    }

    Addr(const std::string& ip, const uint16_t port) : _ip(ip), _port(port) 
    {  
        _naddr.addr.ip = inet_addr(ip.c_str());
        _naddr.addr.port = port;
    }

    Addr(const uint64_t n)
    {
        _naddr.n = n;

        struct in_addr na;
        memcpy(&na, &_naddr.addr.ip, sizeof(_naddr.addr.ip));
        _ip = inet_ntoa(na);
        _port = _naddr.addr.port;
    }

    void set(const std::string& ip, const uint16_t port)
    {
        _ip = ip;
        _port = port;
        _naddr.addr.ip = inet_addr(ip.c_str());
        _naddr.addr.port = port;
    }

    void set(const uint64_t n)
    {
        _naddr.n = n;

        struct in_addr na;
        memcpy(&na, &_naddr.addr.ip, sizeof(_naddr.addr.ip));
        _ip = inet_ntoa(na);
        _port = _naddr.addr.port;
    }

    void set(const string& ipport)
    {
        _port = 0;
        _naddr = 0;

        size_t seperator = ipport.find(':');
        if (seperator == std::string::npos)
        {
            _ip = ipport;
        }
        else
        {
            _ip = ipport.substr(0, seperator);
            _port = (uint16_t)atoi(ipport.substr(seperator + 1).c_str());
        }

        _naddr.addr.ip = inet_addr(_ip.c_str());
        _naddr.addr.port = _port;
    }

    bool operator < (const Addr& r) const
    {
        return _naddr.n < r._naddr.n;
    }

    bool operator == (const Addr& r) const
    {
        return _naddr.n == _naddr.n;
    }

} Addr;

}

#endif
