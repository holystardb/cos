#ifndef _OS_LIST_H
#define _OS_LIST_H

#ifdef __cplusplus
extern "C" {
#endif


#define UT_LIST_BASE_NODE_T(TYPE)\
struct {\
	uint32	count;	/* count of nodes in list */\
	TYPE *	start;	/* pointer to list start, NULL if empty */\
	TYPE *	end;	/* pointer to list end, NULL if empty */\
}\

#define UT_LIST_NODE_T(TYPE)\
struct {\
	TYPE *	prev;	/* pointer to the previous node, NULL if start of list */\
	TYPE *	next;	/* pointer to next node, NULL if end of list */\
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


#ifdef __cplusplus
}
#endif

#endif  /* _OS_LIST_H */

