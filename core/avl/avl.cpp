#include <assert.h>
#include "avl.h"

static uint32_t max(uint32_t a, uint32_t b)
{
    return a > b ? a : b;
}

// maintain height and count
static void avl_update(AVLNode *node)
{
    node->height = max(avl_height(node->left), avl_height(node->right)) + 1;
    node->cnt = avl_cnt(node->left) + avl_cnt(node->right) + 1;
}

static AVLNode *rot_left(AVLNode *node)
{
    AVLNode *parent = node->parent;
    AVLNode *new_node = node->right;
    AVLNode *inner = new_node->left;
    node->right = inner;
    if (inner)
        inner->parent = node;

    new_node->parent = parent;
    new_node->left = node;
    node->parent = new_node;

    avl_update(node);
    avl_update(new_node);
    return new_node;
}

static AVLNode *rot_right(AVLNode *node)
{
    AVLNode *parent = node->parent;
    AVLNode *new_node = node->left;
    AVLNode *inner = new_node->right;
    node->left = inner;
    if (inner)
        inner->parent = node;

    new_node->parent = parent;
    new_node->right = node;
    node->parent = new_node;

    avl_update(node);
    avl_update(new_node);
    return new_node;
}

// the left subtree is taller by 2
static AVLNode *avl_fix_left(AVLNode *node)
{
    if (avl_height(node->left->left) < avl_height(node->left->right))
        node->right = rot_left(node->left);
    return rot_right(node);
}

// the right subtree is taller by 2
static AVLNode *avl_fix_right(AVLNode *node)
{
    if (avl_height(node->right->left) > avl_height(node->right->right))
        node->right = rot_right(node->right);
    return rot_left(node);
}

// fix imbalanced nodes and maintain invariants until the root is reached
AVLNode *avl_fix(AVLNode *node)
{
    while (true)
    {
        AVLNode **from = &node; // save fixed subtree
        AVLNode *parent = node->parent;
        if (parent)
        {
            // attach fixed subtree to parent
            from = parent->left == node ? &parent->left : &parent->right;
        }
        avl_update(node);
        // fix height diff
        uint32_t left = avl_height(node->left);
        uint32_t right = avl_height(node->right);

        if (left == right + 2)
        {
            *from = avl_fix_left(node);
        }
        else if (left + 2 == right)
        {
            *from = avl_fix_right(node);
        }

        // if root, stop
        if (!parent)
            return *from;

        // continue with parent coz now it might be imbalanced
        node = parent;
    }
}

// detach node where 1 child is NULL
AVLNode *avl_del_easy(AVLNode *node)
{
    assert(!node->left || !node->right); // atmost 1 child
    AVLNode *parent = node->parent;
    AVLNode *child = node->left ? node->left : node->right;
    if (child)
        child->parent = parent;

    if (!parent)
        return child; // if node was root, return child as new root
    AVLNode **from = parent->left == node ? &parent->left : &parent->right;
    *from = child; // attach child to parent
    return avl_fix(parent);
}

// detach node and return new root
AVLNode *avl_del(AVLNode *node)
{
    // 0 or 1 child
    if (!node->left || !node->right)
        return avl_del_easy(node);

    // find successor
    AVLNode *succ = node->right;
    while (succ->left)
    {
        succ = succ->left;
    }
    // detach succ
    AVLNode *root = avl_del_easy(succ);
    // swap with succ
    *succ = *node;
    if (succ->left)
        succ->left->parent = succ;
    if (succ->right)
        succ->right->parent = succ;

    // attach succ to parent, update root
    AVLNode **from = &root;
    AVLNode *parent = node->parent;
    if (parent)
        from = parent->left == node ? &parent->left : &parent->right;
    *from = succ;
    return root;
}