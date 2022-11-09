#ifndef _CM_RBT_H
#define _CM_RBT_H

#include "cm_type.h"
#include "cm_memory.h"

#ifdef __cplusplus
extern "C" {
#endif

#define ut_alloc_node   alloc_rbt_node
#define ut_free_node    free_rbt_node


/* Red black tree typedefs */
typedef struct ib_rbt_struct ib_rbt_t;
typedef struct ib_rbt_node_struct ib_rbt_node_t;
/* FIXME: Iterator is a better name than _bound_ */
typedef struct ib_rbt_bound_struct ib_rbt_bound_t;
typedef void (*ib_rbt_print_node)(const ib_rbt_node_t* node);
typedef int (*ib_rbt_compare)(const void* p1, const void* p2);

/** Red black tree color types */
enum ib_rbt_color_enum {
	IB_RBT_RED,
	IB_RBT_BLACK
};

typedef enum ib_rbt_color_enum ib_rbt_color_t;

/** Red black tree node */
struct ib_rbt_node_struct {
	ib_rbt_color_t	color;			/* color of this node */

	ib_rbt_node_t*	left;			/* points left child */
	ib_rbt_node_t*	right;			/* points right child */
	ib_rbt_node_t*	parent;			/* points parent node */

	char		value[1];		/* Data value */
};

/** Red black tree instance.*/
struct	ib_rbt_struct {
	ib_rbt_node_t*	nil;			/* Black colored node that is
						used as a sentinel. This is
						pre-allocated too.*/

	ib_rbt_node_t*	root;			/* Root of the tree, this is
						pre-allocated and the first
						data node is the left child.*/

	uint32		n_nodes;		/* Total number of data nodes */

	ib_rbt_compare	compare;		/* Fn. to use for comparison */
	size_t		sizeof_value;		/* Sizeof the item in bytes */
	
	uint32	count;	/* count of nodes in list */
	ib_rbt_node_t*	start;	/* pointer to list start, NULL if empty */
	ib_rbt_node_t*	end;	/* pointer to list end, NULL if empty */
};

/** The result of searching for a key in the tree, this is useful for
a speedy lookup and insert if key doesn't exist.*/
struct ib_rbt_bound_struct {
	const ib_rbt_node_t*
			last;			/* Last node visited */

	int		result;			/* Result of comparing with
						the last non-nil node that was visited */
};

/* Size in elements (t is an rb tree instance) */
#define rbt_size(t)	(t->n_nodes)

/* Check whether the rb tree is empty (t is an rb tree instance) */
#define rbt_empty(t)	(rbt_size(t) == 0)

/* Get data value (t is the data type, n is an rb tree node instance) */
#define rbt_value(t, n) ((t*) &n->value[0])

/* Compare a key with the node value (t is tree, k is key, n is node)*/
#define rbt_compare(t, k, n) (t->compare(k, n->value))

/**********************************************************************//**
Free an instance of  a red black tree */

void
rbt_free(
/*=====*/
	ib_rbt_t*	tree);			/*!< in: rb tree to free */
/**********************************************************************//**
Create an instance of a red black tree
@return	rb tree instance */

ib_rbt_t*
rbt_create(
/*=======*/
	size_t		sizeof_value,		/*!< in: size in bytes */
	ib_rbt_compare	compare);		/*!< in: comparator */
/**********************************************************************//**
Delete a node from the red black tree, identified by key */

bool32
rbt_delete(
/*=======*/
						/* in: TRUE on success */
	ib_rbt_t*	tree,			/* in: rb tree */
	const void*	key);			/* in: key to delete */
/**********************************************************************//**
Remove a node from the red black tree, NOTE: This function will not delete
the node instance, THAT IS THE CALLERS RESPONSIBILITY.
@return	the deleted node with the const. */

ib_rbt_node_t*
rbt_remove_node(
/*============*/
	ib_rbt_t*	tree,			/*!< in: rb tree */
	const ib_rbt_node_t*
			node);			/*!< in: node to delete, this
						is a fudge and declared const
						because the caller has access
						only to const nodes.*/
/**********************************************************************//**
Return a node from the red black tree, identified by
key, NULL if not found
@return	node if found else return NULL */

const ib_rbt_node_t*
rbt_lookup(
/*=======*/
	const ib_rbt_t*	tree,			/*!< in: rb tree to search */
	const void*	key);			/*!< in: key to lookup */
/**********************************************************************//**
Add data to the red black tree, identified by key (no dups yet!)
@return	inserted node */

const ib_rbt_node_t*
rbt_insert(
/*=======*/
	ib_rbt_t*	tree,			/*!< in: rb tree */
	const void*	key,			/*!< in: key for ordering */
	const void*	value);			/*!< in: data that will be
						copied to the node.*/
/**********************************************************************//**
Add a new node to the tree, useful for data that is pre-sorted.
@return	appended node */

const ib_rbt_node_t*
rbt_add_node(
/*=========*/
	ib_rbt_t*	tree,			/*!< in: rb tree */
	ib_rbt_bound_t*	parent,			/*!< in: parent */
	const void*	value);			/*!< in: this value is copied
						to the node */
/**********************************************************************//**
Return the left most data node in the tree
@return	left most node */

const ib_rbt_node_t*
rbt_first(
/*======*/
	const ib_rbt_t*	tree);			/*!< in: rb tree */
/**********************************************************************//**
Return the right most data node in the tree
@return	right most node */

const ib_rbt_node_t*
rbt_last(
/*=====*/
	const ib_rbt_t*	tree);			/*!< in: rb tree */
/**********************************************************************//**
Return the next node from current.
@return	successor node to current that is passed in. */

const ib_rbt_node_t*
rbt_next(
/*=====*/
	const ib_rbt_t*	tree,			/*!< in: rb tree */
	const ib_rbt_node_t*			/* in: current node */
			current);
/**********************************************************************//**
Return the prev node from current.
@return	precedessor node to current that is passed in */

const ib_rbt_node_t*
rbt_prev(
/*=====*/
	const ib_rbt_t*	tree,			/*!< in: rb tree */
	const ib_rbt_node_t*			/* in: current node */
			current);
/**********************************************************************//**
Find the node that has the lowest key that is >= key.
@return	node that satisfies the lower bound constraint or NULL */

const ib_rbt_node_t*
rbt_lower_bound(
/*============*/
	const ib_rbt_t*	tree,			/*!< in: rb tree */
	const void*	key);			/*!< in: key to search */
/**********************************************************************//**
Find the node that has the greatest key that is <= key.
@return	node that satisifies the upper bound constraint or NULL */

const ib_rbt_node_t*
rbt_upper_bound(
/*============*/
	const ib_rbt_t*	tree,			/*!< in: rb tree */
	const void*	key);			/*!< in: key to search */
/**********************************************************************//**
Search for the key, a node will be retuned in parent.last, whether it
was found or not. If not found then parent.last will contain the
parent node for the possibly new key otherwise the matching node.
@return	result of last comparison */

int
rbt_search(
/*=======*/
	const ib_rbt_t*	tree,			/*!< in: rb tree */
	ib_rbt_bound_t*	parent,			/*!< in: search bounds */
	const void*	key);			/*!< in: key to search */
/**********************************************************************//**
Search for the key, a node will be retuned in parent.last, whether it
was found or not. If not found then parent.last will contain the
parent node for the possibly new key otherwise the matching node.
@return	result of last comparison */

int
rbt_search_cmp(
/*===========*/
	const ib_rbt_t*	tree,			/*!< in: rb tree */
	ib_rbt_bound_t*	parent,			/*!< in: search bounds */
	const void*	key,			/*!< in: key to search */
	ib_rbt_compare	compare);		/*!< in: comparator */
/**********************************************************************//**
Clear the tree, deletes (and free) all the nodes. */

void
rbt_clear(
/*======*/
	ib_rbt_t*	tree);			/*!< in: rb tree */
/**********************************************************************//**
Merge the node from dst into src. Return the number of nodes merged.
@return	no. of recs merged */

uint32
rbt_merge_uniq(
/*===========*/
	ib_rbt_t*	dst,			/*!< in: dst rb tree */
	const ib_rbt_t*	src);			/*!< in: src rb tree */
/**********************************************************************//**
Merge the node from dst into src. Return the number of nodes merged.
Delete the nodes from src after copying node to dst. As a side effect
the duplicates will be left untouched in the src, since we don't support
duplicates (yet). NOTE: src and dst must be similar, the function doesn't
check for this condition (yet).
@return	no. of recs merged */

uint32
rbt_merge_uniq_destructive(
/*=======================*/
	ib_rbt_t*	dst,			/*!< in: dst rb tree */
	ib_rbt_t*	src);			/*!< in: src rb tree */
/**********************************************************************//**
Verify the integrity of the RB tree. For debugging. 0 failure else height
of tree (in count of black nodes).
@return	TRUE if OK FALSE if tree invalid. */

bool32
rbt_validate(
/*=========*/
	const ib_rbt_t*	tree);			/*!< in: tree to validate */
/**********************************************************************//**
Iterate over the tree in depth first order. */

void
rbt_print(
/*======*/
	const ib_rbt_t*		tree,		/*!< in: tree to traverse */
	ib_rbt_print_node	print);		/*!< in: print function */


#ifdef __cplusplus
}
#endif

#endif /* _CM_RBT_H */
