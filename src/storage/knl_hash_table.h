#ifndef _KNL_HASH_TABLE_H
#define _KNL_HASH_TABLE_H

#include "cm_type.h"
#include "cm_memory.h"
#include "cm_rwlock.h"

/* Differnt types of hash_table based on the synchronization method used for it. */
enum hash_table_sync_t {
  HASH_TABLE_SYNC_NONE = 0, // Don't use any internal synchronization objects for this hash_table
  HASH_TABLE_SYNC_MUTEX,    // Use mutexes to control access to this hash_table
  HASH_TABLE_SYNC_RW_LOCK   // Use rw_locks to control access to this hash_table
};

typedef void* HASH_NODE_T;

typedef struct ST_HASH_CELL{
    void    *node; /*!< hash chain node, NULL if none */
} HASH_CELL_T;

typedef struct ST_HASH_TABLE {
    enum hash_table_sync_t type; /*!< type of hash_table. */

    HASH_CELL_T         *array; /* pointer to cell array */
    uint32             n_cells; /* number of cells in the hash table */
    uint32 n_sync_obj; /* if sync_objs != NULL, then the number of either the number of mutexes or 
                          the number of rw_locks depending on the type. Must be a power of 2 */
    union {
      mutex_t *mutexes; /* NULL, or an array of mutexes used to protect segments of the hash table */
      rw_lock_t *rw_locks; /* NULL, or an array of rw_lcoks used to protect segments of the hash table */
    } sync_obj;

    uint32             magic_n;
#define HASH_TABLE_MAGIC_N  76561114
} HASH_TABLE;

#define HASH_ASSERT_OWN(TABLE, FOLD)                    \
    ut_ad((TABLE)->type != HASH_TABLE_SYNC_MUTEX   || (mutex_own(HASH_GET_MUTEX((TABLE), FOLD))));

#define HASH_INSERT(TYPE, NAME, TABLE, FOLD, DATA)                         \
    do {                                                                   \
        HASH_CELL_T *cell3333;                                               \
        TYPE *struct3333;                                                  \
        HASH_ASSERT_OWN(TABLE, FOLD)                                       \
        (DATA)->NAME = NULL;                                               \
        cell3333 = HASH_GET_NTH_CELL(TABLE, HASH_CALC_HASH(TABLE, FOLD));  \
        if (cell3333->node == NULL) {                                      \
            cell3333->node = DATA;                                         \
        } else {                                                           \
            struct3333 = (TYPE*)cell3333->node;                            \
            while (struct3333->NAME != NULL) {                             \
                struct3333 = (TYPE*) struct3333->NAME;                     \
            }                                                              \
            struct3333->NAME = DATA;                                       \
        }                                                                  \
    } while (0)

#ifdef UNIV_HASH_DEBUG
#define HASH_ASSERT_VALID(DATA) ut_a((void *)(DATA) != (void *)-1)
#define HASH_INVALIDATE(DATA, NAME) *(void **)(&DATA->NAME) = (void *)-1
#else
#define HASH_ASSERT_VALID(DATA)     \
  do {                              \
  } while (0)

#define HASH_INVALIDATE(DATA, NAME) \
  do {                              \
  } while (0)
#endif

#define HASH_DELETE(TYPE, NAME, TABLE, FOLD, DATA)                         \
do {                                                                       \
    HASH_CELL_T *cell3333;                                                 \
    TYPE *struct3333;                                                      \
    HASH_ASSERT_OWN(TABLE, FOLD)                                           \
    cell3333 = HASH_GET_NTH_CELL(TABLE, HASH_CALC_HASH(TABLE, FOLD));      \
    if (cell3333->node == DATA) {                                          \
        cell3333->node = DATA->NAME;                                       \
    } else {                                                               \
        struct3333 = (TYPE*) cell3333->node;                               \
        while (struct3333->NAME != DATA) {                                 \
            struct3333 = (TYPE*) struct3333->NAME;                         \
            ut_a(struct3333);                                              \
        }                                                                  \
        struct3333->NAME = DATA->NAME;                                     \
    }                                                                      \
} while (0)

#define HASH_GET_FIRST(TABLE, HASH_VAL)   (HASH_GET_NTH_CELL(TABLE, HASH_VAL)->node)

#define HASH_GET_NEXT(NAME, DATA)         ((DATA)->NAME)

#define HASH_SEARCH(NAME, TABLE, FOLD, TYPE, DATA, ASSERTION, TEST)        \
{                                                                          \
    HASH_ASSERT_OWN(TABLE, FOLD)                                           \
    (DATA) = (TYPE) HASH_GET_FIRST(TABLE, HASH_CALC_HASH(TABLE, FOLD));    \
    while ((DATA) != NULL) {                                               \
        ASSERTION;                                                         \
        if (TEST) {                                                        \
            break;                                                         \
        } else {                                                           \
            (DATA) = (TYPE) HASH_GET_NEXT(NAME, DATA);                     \
        }                                                                  \
    }                                                                      \
}

#define HASH_SEARCH_ALL(NAME, TABLE, TYPE, DATA, ASSERTION, TEST)          \
do {                                                                       \
    uint32 i3333;                                                          \
    for (i3333 = (TABLE)->n_cells; i3333--; ) {                            \
        (DATA) = (TYPE) HASH_GET_FIRST(TABLE, i3333);                      \
        while ((DATA) != NULL) {                                           \
            ASSERTION;                                                     \
            if (TEST) {                                                    \
                break;                                                     \
            }                                                              \
            (DATA) = (TYPE) HASH_GET_NEXT(NAME, DATA);                     \
        }                                                                  \
        if ((DATA) != NULL) {                                              \
            break;                                                         \
        }                                                                  \
    }                                                                      \
} while (0)


extern HASH_TABLE* HASH_TABLE_CREATE(uint32 hash_cell_count,
    enum hash_table_sync_t type, // in: HASH_TABLE_SYNC_MUTEX or HASH_TABLE_SYNC_RW_LOCK
    uint32 n_sync_obj); // in: number of sync objects, must be a power of 2

extern void HASH_TABLE_FREE(HASH_TABLE* table);

extern HASH_CELL_T* HASH_GET_NTH_CELL(HASH_TABLE* table, uint32 n);
extern uint32 HASH_CALC_HASH(HASH_TABLE* table, uint32 fold);

extern mutex_t* HASH_GET_MUTEX(HASH_TABLE* table, uint32 fold);

extern rw_lock_t* hash_get_lock(HASH_TABLE* table, uint32 fold);
extern rw_lock_t* hash_lock_s_confirm(rw_lock_t* hash_lock, HASH_TABLE* table, uint32 fold);
extern rw_lock_t* hash_lock_x_confirm(rw_lock_t* hash_lock, HASH_TABLE* table, uint32 fold);


#endif  /* _KNL_HASH_TABLE_H */
