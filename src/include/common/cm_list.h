#ifndef _CM_LIST_H
#define _CM_LIST_H

#ifdef __cplusplus
extern "C" {
#endif


#define UT_LIST_BASE_NODE_T(TYPE)\
struct {\
    uint32 count; /* count of nodes in list */\
    TYPE *start;  /* pointer to list start, NULL if empty */\
    TYPE *end;    /* pointer to list end, NULL if empty */\
}\

#define UT_LIST_NODE_T(TYPE)\
struct {\
    TYPE *prev; /* pointer to the previous node, NULL if start of list */\
    TYPE *next; /* pointer to next node, NULL if end of list */\
}\

#define UT_LIST_INIT(BASE)\
{\
    (BASE).count = 0;\
    (BASE).start = NULL;\
    (BASE).end   = NULL;\
}\

#define UT_LIST_ADD_FIRST(NAME, BASE, N)\
{\
    ((BASE).count)++;\
    ((N)->NAME).next = (BASE).start;\
    ((N)->NAME).prev = NULL;\
    if ((BASE).start != NULL) {\
        (((BASE).start)->NAME).prev = (N);\
    }\
    (BASE).start = (N);\
    if ((BASE).end == NULL) {\
        (BASE).end = (N);\
    }\
}\

#define UT_LIST_ADD_LAST(NAME, BASE, N)\
{\
    ((BASE).count)++;\
    ((N)->NAME).prev = (BASE).end;\
    ((N)->NAME).next = NULL;\
    if ((BASE).end != NULL) {\
        (((BASE).end)->NAME).next = (N);\
    }\
    (BASE).end = (N);\
    if ((BASE).start == NULL) {\
        (BASE).start = (N);\
    }\
}\

#define UT_LIST_ADD_AFTER(NAME, BASE, POS_ELEM, ELEM)\
{\
    ut_ad((POS_ELEM) != NULL);\
    ut_ad((ELEM) != NULL);\
    ut_ad((POS_ELEM) != (ELEM));\
    ((ELEM)->NAME).next = ((POS_ELEM)->NAME).next;\
    ((POS_ELEM)->NAME).next = (ELEM);\
    ((ELEM)->NAME).prev = (POS_ELEM);\
    if ((BASE).end == (POS_ELEM)) {\
        (BASE).end = (ELEM);\
    }\
    ((BASE).count)++;\
}\

#define UT_LIST_ADD_BEFORE(NAME, BASE, POS_ELEM, ELEM)\
{\
    ut_ad((POS_ELEM) != NULL);\
    ut_ad((ELEM) != NULL);\
    ut_ad((POS_ELEM) != (ELEM));\
    ((ELEM)->NAME).next = (POS_ELEM);\
    ((ELEM)->NAME).prev = ((POS_ELEM)->NAME).prev;\
    if ((BASE).start != (POS_ELEM)) {\
        ((((POS_ELEM)->NAME).prev)->NAME).next = (ELEM);\
        ((POS_ELEM)->NAME).prev = (ELEM);\
    } else {\
        (BASE).start = (ELEM);\
    }\
    ((BASE).count)++;\
}\

#define UT_LIST_REMOVE(NAME, BASE, N)\
{\
    ((BASE).count)--;\
    if (((N)->NAME).next != NULL) {\
        ((((N)->NAME).next)->NAME).prev = ((N)->NAME).prev;\
    } else {\
        (BASE).end = ((N)->NAME).prev;\
    }\
    if (((N)->NAME).prev != NULL) {\
        ((((N)->NAME).prev)->NAME).next = ((N)->NAME).next;\
    } else {\
        (BASE).start = ((N)->NAME).next;\
    }\
}\

#define UT_LIST_GET_NEXT(NAME, N)\
    (((N)->NAME).next)

#define UT_LIST_GET_PREV(NAME, N)\
    (((N)->NAME).prev)

#define UT_LIST_GET_LEN(BASE)\
    (BASE).count

#define UT_LIST_GET_FIRST(BASE)\
    (BASE).start

#define UT_LIST_GET_LAST(BASE)\
    (BASE).end


// ---------------------------------------------------------------------------------------

#define SLIST_BASE_NODE_T(TYPE)        \
    struct {                           \
        uint32 count;                  \
        TYPE *first;                   \
        TYPE *last;                    \
    }                                  \

#define SLIST_NODE_T(TYPE)             \
    struct {                           \
        TYPE *next;                    \
    }                                  \

#define SLIST_INIT(BASE)         \
    {                            \
        (BASE).first = NULL;     \
        (BASE).last = NULL;      \
        (BASE).count = 0;        \
    }                            \

#define SLIST_GET_LEN(BASE)       (BASE).count

#define SLIST_GET_FIRST(BASE)     (BASE).first

#define SLIST_GET_LAST(BASE)      (BASE).last

#define SLIST_GET_NEXT(NAME, N)   (((N)->NAME).next)


#define SLIST_GET_AND_REMOVE_FIRST(NAME, BASE, N)    \
    {                                                \
        (N) = (BASE).first;                          \
        if (LIKELY(N != NULL)) {                             \
            (BASE).first = ((N)->NAME).next;         \
            if (UNLIKELY((N) == (BASE).last)) {      \
                ut_a((BASE).count == 1);             \
                (BASE).last = NULL;                  \
            }                                        \
            (BASE).count--;                          \
        } else {                                     \
            ut_a((BASE).count == 0);                 \
            ut_a((BASE).last == NULL);               \
        }                                            \
    }                                                \

#define SLIST_REMOVE_FIRST(NAME, BASE)                   \
    {                                                    \
        if (LIKELY((BASE).first != NULL)) {              \
            ut_a((BASE).count > 0);                      \
            (BASE).first = (((BASE).first)->NAME).next;  \
            (BASE).count--;                              \
            if (UNLIKELY((BASE).count == 0)) {           \
                (BASE).last = NULL;                      \
            }                                            \
        } else {                                         \
            ut_a((BASE).count == 0);                     \
            ut_a((BASE).last == NULL);                   \
        }                                                \
    }                                                    \

#define SLIST_REMOVE(NAME, BASE, PREV, N)            \
    {                                                \
        ut_a((N) != NULL);                           \
        if (UNLIKELY((PREV) == NULL)) {              \
            (BASE).first = ((N)->NAME).next;         \
        } else {                                     \
            ((PREV)->NAME).next = ((N)->NAME).next;  \
        }                                            \
        if ((BASE).last == (N)) {                    \
            (BASE).last = (PREV);                    \
        }                                            \
        (BASE).count--;                              \
    }                                                \

#define SLIST_ADD_LAST(NAME, BASE, N)              \
    {                                              \
        if (UNLIKELY((BASE).first == NULL)) {      \
            ut_a((BASE).count == 0);               \
            (BASE).first = (N);                    \
        }                                          \
        if (LIKELY((BASE).last != NULL)) {         \
            ((BASE).last->NAME).next = (N);        \
        }                                          \
        (BASE).last = (N);                         \
        ((N)->NAME).next = NULL;                   \
        (BASE).count++;                            \
    }                                              \

#define SLIST_ADD_FIRST(NAME, BASE, N)             \
    {                                              \
        ((N)->NAME).next = (BASE).first;           \
        (BASE).first = (N);                        \
        if (UNLIKELY((BASE).last == NULL)) {       \
            ut_a((BASE).count == 0);               \
            (BASE).last = (N);                     \
        }                                          \
        (BASE).count++;                            \
    }                                              \

#define SLIST_APPEND_SLIST(NAME, BASE1, BASE2)          \
    {                                                   \
        if (UNLIKELY((BASE1).first == NULL)) {          \
            ut_a((BASE1).count == 0);                   \
            (BASE1).first = (BASE2).first;              \
        }                                               \
        if (LIKELY((BASE1).last != NULL)) {             \
            ((BASE1).last->NAME).next = (BASE2).first;  \
        }                                               \
        if (LIKELY((BASE2).last != NULL)) {             \
            (BASE1).last = (BASE2).last;                \
        }                                               \
        (BASE1).count += (BASE2).count;                 \
    }                                                   \


#ifdef __cplusplus
}
#endif

#endif  /* _CM_LIST_H */

