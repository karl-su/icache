#include <sys/time.h>
#include <stddef.h>
#include <string.h>

#include "util.h"

uint64_t Util::us()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);

    return tv.tv_sec * 1000000 + tv.tv_usec;
}

uint64_t Util::ms()
{
    return Util::us() / 1000;
}

uint64_t Util::ts()
{
    return Util::us() / 1000000;
}

void Util::separate(const std::string& str, char tag, std::vector<std::string>& vec)
{
    int last = 0;
    
    for (int i = 0; i < (int)str.size(); i++)
    {
        if (str[i] == tag)
        {
            vec.push_back(str.substr(last, i - tag));
            last = i + 1;
        }
    }
}

void Util::separate(const std::string& str, const std::string& tag, std::vector<std::string>& vec)
{
    std::string::size_type pos = 0, next = 0;
    while (pos < str.length())
    {
        next = str.find_first_of(tag, pos);
        if (next == std::string::npos)
        {
            vec.push_back(str.substr(pos));
            return ;
        }

        vec.push_back(str.substr(pos, next - pos));
        pos = next + tag.size();
    }
}

std::string Util::trim(const std::string& str, const char chs[])
{
    int chslen = strlen(chs);
    int start = 0;
    for (int i = 0; i < (int)str.size(); i++)
    {
        bool match = false;
        for (int j = 0; j < chslen; j++)
        {
            if (str[i] == chs[j])
            {
                match = true;
                break;
            }
        }

        start = i;
        if (!match)
            break;
    }

    if (start + 1 >= (int)str.size())
        return "";

    int end = (int)str.size() - 1;
    for (int i = (int)str.size() - 1; i > start; i--)
    {
        bool match = false;
        for (int j = 0; j < chslen; j++)
        {
            if (str[i] == chs[j])
            {
                match = true;
                break;
            }
        }

        end = i;
        if (!match)
            break;
    }

    return str.substr(start, end - start);
}

