#ifndef _KNL_FLIST_H
#define _KNL_FLIST_H

#include "knl_mtr.h"
#include "knl_fsp.h"

/* The C 'types' of base node and list node:
these should be used to write self-documenting code.
Of course, the sizeof macro cannot be applied to these types! */

typedef byte flst_base_node_t;
typedef byte flst_node_t;

/* The physical size of a list base node in bytes */
#define FLST_BASE_NODE_SIZE (4 + 2 * FIL_ADDR_SIZE)

/* The physical size of a list node in bytes */
#define FLST_NODE_SIZE  (2 * FIL_ADDR_SIZE)

/* We define the field offsets of a node for the list */
#define FLST_PREV 0 /* 6-byte address of the previous list element;
the page part of address is FIL_NULL, if no previous element */
#define FLST_NEXT FIL_ADDR_SIZE /* 6-byte address of the next
list element; the page part of address is FIL_NULL, if no next element */

/* We define the field offsets of a base node for the list */
#define FLST_LEN 0 /* 32-bit list length field */
#define FLST_FIRST 4 /* 6-byte address of the first element
of the list; undefined if empty list */
#define FLST_LAST (4 + FIL_ADDR_SIZE) /* 6-byte address of the
first element of the list; undefined if empty list */

/* Initializes a list base node. */
void flst_init(
    flst_base_node_t* base, /* in: pointer to base node */
    mtr_t*   mtr); /* in: mini-transaction handle */

/* Adds a node as the last node in a list. */
void flst_add_last(
    flst_base_node_t* base, /* in: pointer to base node of list */
    flst_node_t*  node, /* in: node to add */
    mtr_t*   mtr); /* in: mini-transaction handle */

/* Adds a node as the first node in a list. */
void flst_add_first(
    flst_base_node_t* base, /* in: pointer to base node of list */
    flst_node_t* node, /* in: node to add */
    mtr_t* mtr); /* in: mini-transaction handle */

/* Inserts a node after another in a list. */
void flst_insert_after(
    flst_base_node_t* base, /* in: pointer to base node of list */
    flst_node_t*  node1, /* in: node to insert after */
    flst_node_t*  node2, /* in: node to add */
    mtr_t*   mtr); /* in: mini-transaction handle */

/* Inserts a node before another in a list. */
void flst_insert_before(
    flst_base_node_t* base, /* in: pointer to base node of list */
    flst_node_t*  node2, /* in: node to insert */
    flst_node_t*  node3, /* in: node to insert before */
    mtr_t*   mtr); /* in: mini-transaction handle */

/* Removes a node. */
void flst_remove(
    flst_base_node_t* base, /* in: pointer to base node of list */
    flst_node_t*  node2, /* in: node to remove */
    mtr_t*   mtr); /* in: mini-transaction handle */

/* Cuts off the tail of the list, including the node given.
The number of nodes which will be removed must be provided by the caller,
as this function does not measure the length of the tail. */
void flst_cut_end(
    flst_base_node_t* base, /* in: pointer to base node of list */
    flst_node_t* node2, /* in: first node to remove */
    uint32 n_nodes,/* in: number of nodes to remove, must be >= 1 */
    mtr_t* mtr); /* in: mini-transaction handle */

/* Cuts off the tail of the list, not including the given node.
The number of nodes which will be removed must be provided by the caller, as this function does not measure the length of the tail. */
void flst_truncate_end(
    flst_base_node_t* base, /* in: pointer to base node of list */
    flst_node_t*  node2, /* in: first node not to remove */
    uint32   n_nodes,/* in: number of nodes to remove */
    mtr_t*   mtr); /* in: mini-transaction handle */

/* Gets list length. */
uint32 flst_get_len(
    flst_base_node_t* base, /* in: pointer to base node */
    mtr_t*   mtr); /* in: mini-transaction handle */

/* Gets list first node address. */
fil_addr_t flst_get_first(
    flst_base_node_t* base, /* in: pointer to base node */
    mtr_t*   mtr); /* in: mini-transaction handle */

/* Gets list last node address. */
fil_addr_t flst_get_last(
    flst_base_node_t* base, /* in: pointer to base node */
    mtr_t*   mtr); /* in: mini-transaction handle */

/* Gets list next node address. */
fil_addr_t flst_get_next_addr(
    flst_node_t* node, /* in: pointer to node */
    mtr_t*  mtr); /* in: mini-transaction handle */

/* Gets list prev node address. */
fil_addr_t flst_get_prev_addr(
    flst_node_t* node, /* in: pointer to node */
    mtr_t*  mtr); /* in: mini-transaction handle */

/* Writes a file address. */
void flst_write_addr(
    fil_faddr_t* faddr, /* in: pointer to file faddress */
    fil_addr_t addr, /* in: file address */
    mtr_t*  mtr); /* in: mini-transaction handle */

/* Reads a file address. */
fil_addr_t flst_read_addr(
    fil_faddr_t* faddr, /* in: pointer to file faddress */
    mtr_t*  mtr); /* in: mini-transaction handle */

/* Validates a file-based list. */
bool32 flst_validate(
    flst_base_node_t* base, /* in: pointer to base node of list */
    mtr_t*   mtr1); /* in: mtr */

/* Prints info of a file-based list. */
void flst_print(
    flst_base_node_t* base, /* in: pointer to base node of list */
    mtr_t*   mtr); /* in: mtr */

#endif  /* _KNL_FLIST_H */