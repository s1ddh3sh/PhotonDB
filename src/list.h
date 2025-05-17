#pragma once

#include <stddef.h>

struct DList
{
    DList *next = NULL;
    DList *prev = NULL;
};

inline void dlist_init(DList *list)
{
    list->next = list;
    list->prev = list;
}

inline bool dlist_empty(DList *list)
{
    return list->next == list;
}

inline void dlist_detach(DList *node)
{
    DList *prev = node->prev;
    DList *next = node->next;
    prev->next = next;
    next->prev = prev;
}

inline void dlist_insert_before(DList *target, DList *rookie)
{
    DList *prev = target->prev;
    prev->next = rookie;
    rookie->prev = prev;
    rookie->next = target;
    target->prev = rookie;
}