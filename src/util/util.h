#ifndef __UTIL_H__
#define __UTIL_H__

#include <stdint.h>
#include <vector>
#include <string>
#include <sstream>

class Util {
public:
    static uint64_t us();

    static uint64_t ms();

    static uint64_t ts();

    static void separate(const std::string& str, char tag, std::vector<std::string>& vec);

    static void separate(const std::string& str, const std::string& tag, std::vector<std::string>& vec);
    
    static std::string trim(const std::string& str, const char chs[] = " \t\r\n");

    template<class T>
    static std::string tostr(const T& d);
};

template<class T>
std::string Util::tostr(const T& d)
{
    std::ostringstream os;
    os << d;
    return os.str();
}


#endif

