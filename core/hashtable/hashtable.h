#pragma once

#include <stddef.h>
#include <stdint.h>

// hashtable node
typedef struct HNode
{
    HNode *next = NULL;
    uint64_t hcode = 0;
} HNode;

// simple fixed size hashtable
typedef struct HTab
{
    HNode **tab = NULL; // array of slots
    size_t mask = 0;    // 2^n-1 - for optimizing lookup
    size_t size = 0;
} HTab;

// real HTABLE interface
typedef struct HMap
{
    HTab newer;
    HTab older;
    size_t migrate_pos = 0;
} HMap;

HNode *hm_lookup(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *));
void hm_insert(HMap *hmap, HNode *node);
HNode *hm_delete(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *));
void hm_clear(HMap *hmap);
size_t hm_size(HMap *hmap);
void hm_foreach(HMap *hmap, bool (*f)(HNode *, void *), void *arg);