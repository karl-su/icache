#ifndef __RING_QUE_H__
#define __RING_QUE_H__

#include <stdint.h>
#include <assert.h>

template <class T>
class RingQue {
public:
    RingQue(uint32_t size);

    RingQue(const RingQue& q) { *this = q; }

    virtual ~RingQue();

    RingQue& operator= (const RingQue& q);

    bool Empty() { return m_read == m_write; }
    bool Full() { uint32_t r = m_read, w = m_write; return ((w + 1) % (m_size + 1)) == r; }

    uint32_t Len();

    int Push(T& o);

    int Pop(T& o);

protected:
    T*  m_buf;
    uint32_t m_read;
    uint32_t m_write;
    uint32_t m_size;
};

template<class T>
RingQue<T>::RingQue(uint32_t size) : m_buf(NULL), m_read(0), m_write(0), m_size(size)
{
    if (m_size == 0)
        m_size  = 4;
    m_buf = new T[m_size + 1];
    assert(m_buf != NULL);
}

template<class T>
RingQue<T>::~RingQue()
{
    if (m_buf)
    {
        delete m_buf;
        m_buf = NULL;
    }
}

template<class T>
RingQue<T>& RingQue<T>::operator= (const RingQue<T>& q)
{
    if (m_buf)
        delete m_buf;
 
    m_size = q.m_size;
    m_read = q.m_read;
    m_write = q.m_write;
    m_buf = new T[m_size + 1]; 
    assert(m_buf != NULL);

    for (uint32_t i = m_read; i != m_write; i = (i + 1) % (m_size + 1)) 
    {
        m_buf[i] = q.m_buf[i];
    }
}

template<class T>
uint32_t RingQue<T>::Len()
{
    uint32_t r = m_read;
    uint32_t w = m_write;

    if (r == w)
        return 0;

    if (r < w)
        return w - r;

    return w + (m_size + 1 - r);
}

template<class T>
int RingQue<T>::Push(T& o)
{
    if (Full())
        return -1;
    m_buf[m_write++] = o;
    if (m_write > m_size)
        m_write = 0;

    return Len();
}

template<class T>
int RingQue<T>::Pop(T& o)
{
    if (Empty())
        return -1;
    o = m_buf[m_read++];
    if (m_read > m_size)
        m_read = 0;

    return Len();
}

#endif

