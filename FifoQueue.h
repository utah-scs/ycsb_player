#ifndef FIFOQUEUE_H_
#define FIFOQUEUE_H_

#include <cassert>
#include <cstdint>
#include <vector>

/**
 * std::queue is just an adapter on top of some other container (such as deque
 * or vector). Unfortunately, deque allocates lots of little bits of memory (does
 * not play well with StlMemoryManager) and vector has inefficient popping from
 * the front (it must copy everything down so that &v[0] can be used as a pointer
 * to the entire array). This class implements a simple FIFO queue on top of a
 * vector that efficiently pops elements from the front by not promising a linear
 * ordering of elements in memory.
 */
template<class T, class Allocator = std::allocator<T> >
class FifoQueue {
  private:
    std::vector<T, Allocator> v;
    size_t head;
    size_t tail;

    void
    resize()
    {
        // move all elements to 0...size()-1 of a new doubly long vector
        std::vector<T, Allocator> newV(v.size() * 2);
        if (head <= tail) {
            newV.insert(newV.begin(), v.begin() + head, v.begin() + tail);
        } else {
            newV.insert(newV.begin(), v.begin() + head, v.end());
            newV.insert(newV.begin() + (v.end() - (v.begin() + head)), v.begin(), v.begin() + tail);
        }
        head = 0;
        tail = v.size() - 1;
        v = newV;
    }

  public:
    FifoQueue() : v(10), head(0), tail(0) { }

    T
    pop()
    {
        assert(!empty());
        T ret = v[head];
        head = (head + 1) % v.size();
        return ret;
    }

    void
    push(const T& val)
    {
        // double the underlying vector if we're about to fill the last empty slot
        if ((tail + 1) % v.size() == head)
            resize();

        v[tail] = val;
        tail = (tail + 1) % v.size();
    }

    bool
    empty()
    {
        return head == tail;
    }

    size_t
    size()
    {
        if (tail >= head)
            return tail - head;
        else
            return v.size() - head + tail;
    }

    T&
    front()
    {
        assert(!empty());
        return v[head];
    }

    T&
    back()
    {
        assert(!empty());
        if (tail == 0)
            return v[v.size() - 1];
        else
            return v[tail - 1];
    }
};

#endif /* !FIFOQUEUE_H_ */
