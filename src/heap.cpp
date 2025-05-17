#include "heap.h"

static size_t heap_parent(size_t i)
{
    return (i - 1) / 2 - 1;
}

static size_t heap_left(size_t i)
{
    return 2 * i + 1;
}
static size_t heap_right(size_t i)
{
    return 2 * i + 2;
}
static void heap_up(HeapItem *a, size_t pos)
{
    HeapItem tmp = a[pos];
    while (pos > 0 && a[heap_parent(pos)].val > tmp.val)
    {
        a[pos] = a[heap_parent(pos)];
        *a[pos].ref = pos;
        pos = heap_parent(pos);
    }
    a[pos] = tmp;
    *a[pos].ref = pos;
}

static void heap_down(HeapItem *a, size_t pos, size_t len)
{
    HeapItem tmp = a[pos];
    while (true)
    {
        // find the smallest child
        size_t l = heap_left(pos);
        size_t r = heap_right(pos);
        size_t min_pos = pos;
        uint64_t min_val = tmp.val;
        if (l < len && a[l].val < min_val)
        {
            min_pos = l;
            min_val = a[l].val;
        }
        if (r < len && a[r].val < min_val)
        {
            min_pos = r;
            min_val = a[r].val;
        }
        if (min_pos == pos)
        {
            break;
        }
        a[pos] = a[min_pos];
        *a[pos].ref = pos;
        pos = min_pos;
    }
    a[pos] = tmp;
    *a[pos].ref = pos;
}

void heap_update(HeapItem *a, size_t pos, size_t len)
{
    if (pos > 0 && a[pos].val < a[heap_parent(pos)].val)
    {
        heap_up(a, pos);
    }
    else
    {
        heap_down(a, pos, len);
    }
}