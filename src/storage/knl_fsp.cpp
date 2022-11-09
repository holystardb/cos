#include "knl_fsp.h"
#include "cm_util.h"
#include "knl_buf.h"

/* The data structures in files are defined just as byte strings in C */
typedef byte    fsp_header_t;
typedef byte    xdes_t;
typedef byte    page_t;

/* SPACE HEADER
============

File space header data structure: this data structure is contained in the first page of a space.
The space for this header is reserved in every extent descriptor page, but used only in the first. */

#define FSP_HEADER_OFFSET   FIL_PAGE_DATA   /* Offset of the space header within a file page */
/*-------------------------------------*/
#define FSP_NOT_USED        0   /* this field contained a value up to
which we know that the modifications in the database have been flushed to the file space; not used now */
#define	FSP_SIZE            8   /* Current size of the space in pages */
#define	FSP_FREE_LIMIT      12  /* Minimum page number for which the free list has not been initialized:
the pages >= this limit are, by definition free */
#define	FSP_LOWEST_NO_WRITE 16  /* The lowest page offset for which the page has not been written to disk
(if it has been written, we know that the OS has really reserved the physical space for the page) */
#define	FSP_FRAG_N_USED     20  /* number of used pages in the FSP_FREE_FRAG list */
#define	FSP_FREE            24  /* list of free extents */
#define	FSP_FREE_FRAG       (24 + FLST_BASE_NODE_SIZE)
/* list of partially free extents not belonging to any segment */
#define	FSP_FULL_FRAG       (24 + 2 * FLST_BASE_NODE_SIZE)
/* list of full extents not belonging to any segment */
#define FSP_SEG_ID          (24 + 3 * FLST_BASE_NODE_SIZE)
/* 8 bytes which give the first unused segment id */
#define FSP_SEG_INODES_FULL (32 + 3 * FLST_BASE_NODE_SIZE)
/* list of pages containing segment headers, where all the segment inode slots are reserved */
#define FSP_SEG_INODES_FREE (32 + 4 * FLST_BASE_NODE_SIZE)
/* list of pages containing segment headers, where not all the segment header slots are reserved */
/*-------------------------------------*/
/* File space header size */
#define	FSP_HEADER_SIZE     (32 + 5 * FLST_BASE_NODE_SIZE)

#define	FSP_FREE_ADD        4   /* this many free extents are added to the free list from above FSP_FREE_LIMIT at a time */


/* FILE SEGMENT INODE
==================

Segment inode which is created for each segment in a tablespace. NOTE: in
purge we assume that a segment having only one currently used page can be
freed in a few steps, so that the freeing cannot fill the file buffer with
bufferfixed file pages. */

typedef	byte	fseg_inode_t;

#define FSEG_INODE_PAGE_NODE	FSEG_PAGE_DATA
					/* the list node for linking
					segment inode pages */

#define FSEG_ARR_OFFSET		(FSEG_PAGE_DATA + FLST_NODE_SIZE)
/*-------------------------------------*/
#define	FSEG_ID			0	/* 8 bytes of segment id: if this is
					ut_dulint_zero, it means that the
					header is unused */
#define FSEG_NOT_FULL_N_USED	8
					/* number of used segment pages in
					the FSEG_NOT_FULL list */
#define	FSEG_FREE		12
					/* list of free extents of this
					segment */
#define	FSEG_NOT_FULL		(12 + FLST_BASE_NODE_SIZE)
					/* list of partially free extents */
#define	FSEG_FULL		(12 + 2 * FLST_BASE_NODE_SIZE)
					/* list of full extents */
#define	FSEG_MAGIC_N		(12 + 3 * FLST_BASE_NODE_SIZE)
					/* magic number used in debugging */
#define	FSEG_FRAG_ARR		(16 + 3 * FLST_BASE_NODE_SIZE)
					/* array of individual pages
					belonging to this segment in fsp
					fragment extent lists */
#define FSEG_FRAG_ARR_N_SLOTS	(FSP_EXTENT_SIZE / 2)
					/* number of slots in the array for
					the fragment pages */
#define	FSEG_FRAG_SLOT_SIZE	4	/* a fragment page slot contains its
					page number within space, FIL_NULL
					means that the slot is not in use */
/*-------------------------------------*/
#define FSEG_INODE_SIZE	(16 + 3 * FLST_BASE_NODE_SIZE + FSEG_FRAG_ARR_N_SLOTS * FSEG_FRAG_SLOT_SIZE)

#define FSP_SEG_INODES_PER_PAGE	((UNIV_PAGE_SIZE - FSEG_ARR_OFFSET - 10) / FSEG_INODE_SIZE)
				/* Number of segment inodes which fit on a
				single page */

#define FSEG_MAGIC_N_VALUE	97937874
					
#define	FSEG_FILLFACTOR		8	/* If this value is x, then if
					the number of unused but reserved
					pages in a segment is less than
					reserved pages * 1/x, and there are
					at least FSEG_FRAG_LIMIT used pages,
					then we allow a new empty extent to
					be added to the segment in
					fseg_alloc_free_page. Otherwise, we
					use unused pages of the segment. */
					
#define FSEG_FRAG_LIMIT		FSEG_FRAG_ARR_N_SLOTS
					/* If the segment has >= this many
					used pages, it may be expanded by
					allocating extents to the segment;
					until that only individual fragment
					pages are allocated from the space */

#define	FSEG_FREE_LIST_LIMIT	40	/* If the reserved size of a segment
					is at least this many extents, we
					allow extents to be put to the free
					list of the extent: at most
					FSEG_FREE_LIST_MAX_LEN many */
#define	FSEG_FREE_LIST_MAX_LEN	4
					

/* EXTENT DESCRIPTOR
=================

File extent descriptor data structure: contains bits to tell which pages in
the extent are free and which contain old tuple version to clean. */

/*-------------------------------------*/
#define	XDES_ID			0	/* The identifier of the segment
					to which this extent belongs */
#define XDES_FLST_NODE		8	/* The list node data structure
					for the descriptors */
#define	XDES_STATE		(FLST_NODE_SIZE + 8)
					/* contains state information
					of the extent */
#define	XDES_BITMAP		(FLST_NODE_SIZE + 12)
					/* Descriptor bitmap of the pages
					in the extent */
/*-------------------------------------*/
					
#define	XDES_BITS_PER_PAGE	2	/* How many bits are there per page */
#define	XDES_FREE_BIT		0	/* Index of the bit which tells if the page is free */
#define	XDES_CLEAN_BIT		1	/* NOTE: currently not used!
					Index of the bit which tells if there are old versions of tuples on the page */

/* States of a descriptor */
#define XDES_FREE           1   /* extent is in free list of space */
#define XDES_FREE_FRAG      2   /* extent is in free fragment list of space */
#define XDES_FULL_FRAG      3   /* extent is in full fragment list of space */
#define XDES_FSEG           4   /* extent belongs to a segment */

/* File extent data structure size in bytes. The "+ 7 ) / 8" part in the
definition rounds the number of bytes upward. */
#define	XDES_SIZE	(XDES_BITMAP + (FSP_EXTENT_SIZE * XDES_BITS_PER_PAGE + 7) / 8)

/* Offset of the descriptor array on a descriptor page */
#define	XDES_ARR_OFFSET		(FSP_HEADER_OFFSET + FSP_HEADER_SIZE)







/* The file system. This variable is NULL before the module is initialized. */
fil_system_t* fil_system = NULL;

bool32 fil_system_init(memory_pool_t *pool, uint32 max_n_open, uint32 fil_node_max_count)
{
    if (fil_node_max_count > 0xFFFFF) {
        return FALSE;
    }
    fil_node_max_count = fil_node_max_count < 0xFF ? 0xFF : fil_node_max_count;

    fil_system = (fil_system_t *)malloc(
        ut_align8(sizeof(fil_system_t)) + fil_node_max_count * ut_align8(sizeof(fil_node_t *)));
    if (fil_system) {
        fil_system->mem_context = mcontext_create(pool);
        fil_system->open_pending_num = 0;
        fil_system->max_n_open = max_n_open;
        fil_system->fil_node_num = 0;
        fil_system->fil_node_max_count = fil_node_max_count;
        fil_system->fil_nodes = (fil_node_t **)((char *)fil_system + ut_align8(sizeof(fil_system_t)));
        memset(fil_system->fil_nodes, 0x00, fil_node_max_count * ut_align8(sizeof(fil_node_t *)));
        spin_lock_init(&fil_system->lock);
        UT_LIST_INIT(fil_system->fil_spaces);
        UT_LIST_INIT(fil_system->fil_node_lru);
    }
    return TRUE;
}

fil_space_t* fil_space_create(char *name, uint32 space_id, uint32 purpose)
{
    fil_space_t *space, *tmp;

    space = (fil_space_t *)mcontext_alloc(fil_system->mem_context, ut_align8(sizeof(fil_space_t)) + (uint32)strlen(name) + 1);
    if (space) {
        space->name = (char *)space + ut_align8(sizeof(fil_space_t));
        strncpy_s(space->name, strlen(name) + 1, name, strlen(name));
        space->name[strlen(name) + 1] = 0;

        space->id = space_id;
        space->purpose = purpose;
        space->page_size = 0;
        space->n_reserved_extents = 0;
        space->magic_n = M_FIL_SPACE_MAGIC_N;
        spin_lock_init(&space->lock);
        space->io_in_progress = 0;
        UT_LIST_INIT(space->fil_nodes);
        UT_LIST_INIT(space->free_pages);

        spin_lock(&fil_system->lock, NULL);
        tmp = UT_LIST_GET_FIRST(fil_system->fil_spaces);
        while (tmp) {
            if (strcmp(tmp->name, name) == 0 || tmp->id == space_id) {
                break;
            }
            tmp = UT_LIST_GET_NEXT(list_node, tmp);
        }
        if (tmp == NULL) {
            UT_LIST_ADD_LAST(list_node, fil_system->fil_spaces, space);
        }
        spin_unlock(&fil_system->lock);

        if (tmp) { // name or spaceid already exists
            mcontext_free(fil_system->mem_context, (void *)space);
            space = NULL;
        }
    }

    return space;
}

void fil_space_destroy(uint32 space_id)
{
    fil_space_t *space;
    fil_node_t *fil_node;
    UT_LIST_BASE_NODE_T(fil_node_t) fil_node_list;

    ut_a(space->magic_n == M_FIL_SPACE_MAGIC_N);

    spin_lock(&space->lock, NULL);
    fil_node_list.count = space->fil_nodes.count;
    fil_node_list.start = space->fil_nodes.start;
    fil_node_list.end = space->fil_nodes.end;
    UT_LIST_INIT(space->fil_nodes);
    spin_unlock(&space->lock);

    spin_lock(&fil_system->lock, NULL);
    fil_node = UT_LIST_GET_FIRST(fil_node_list);
    while (fil_node != NULL) {
        if (fil_node->is_open) { /* The node is in the LRU list, remove it */
            UT_LIST_REMOVE(lru_list_node, fil_system->fil_node_lru, fil_node);
            fil_node->is_open = FALSE;
        }
        fil_system->fil_nodes[fil_node->id] = NULL;
        fil_node = UT_LIST_GET_NEXT(chain_list_node, fil_node);
    }
    spin_unlock(&fil_system->lock);

    fil_node = UT_LIST_GET_FIRST(fil_node_list);
    while (fil_node != NULL) {
        os_close_file(fil_node->handle);

        UT_LIST_REMOVE(chain_list_node, fil_node_list, fil_node);
        mcontext_free(fil_system->mem_context, (void *)fil_node);

        fil_node = UT_LIST_GET_FIRST(fil_node_list);
    }

    mcontext_free(fil_system->mem_context, (void *)space);
}

fil_node_t* fil_node_create(fil_space_t *space, char *name, uint32 page_max_count, uint32 page_size, bool32 is_extend)
{
    fil_node_t *node, *tmp;

    spin_lock(&fil_system->lock, NULL);
    if (fil_system->fil_node_num >= fil_system->fil_node_max_count) {
        spin_unlock(&fil_system->lock);
        return NULL;
    }
    fil_system->fil_node_num++;
    spin_unlock(&fil_system->lock);

    node = (fil_node_t *)mcontext_alloc(fil_system->mem_context, ut_align8(sizeof(fil_node_t)) + strlen(name) + 1);
    if (node) {
        node->name = (char *)node + ut_align8(sizeof(fil_node_t));
        strcpy(node->name, name);
        node->name[strlen(name) + 1] = 0;

        node->page_max_count = page_max_count;
        node->page_size = page_size;
        node->page_hwm = 0;
        node->handle = OS_FILE_INVALID_HANDLE;
        node->magic_n = M_FIL_NODE_MAGIC_N;
        node->is_open = 0;
        node->is_extend = is_extend;
        node->n_pending = 0;

        spin_lock(&fil_system->lock, NULL);
        for (uint32 i = 0; i < fil_system->fil_node_max_count; i++) {
            if (fil_system->fil_nodes[i] == NULL) {
                node->id = i;
                fil_system->fil_nodes[i] = node;
            }
        }
        spin_unlock(&fil_system->lock);

        spin_lock(&space->lock, NULL);
        tmp = UT_LIST_GET_FIRST(space->fil_nodes);
        while (tmp) {
            if (strcmp(tmp->name, name) == 0) {
                break;
            }
            tmp = UT_LIST_GET_NEXT(chain_list_node, tmp);
        }
        if (tmp == NULL) { // name not already exists
            UT_LIST_ADD_LAST(chain_list_node, space->fil_nodes, node);
        }
        spin_unlock(&space->lock);

        if (tmp) {
            spin_lock(&fil_system->lock, NULL);
            fil_system->fil_nodes[node->id] = NULL;
            fil_system->fil_node_num--;
            spin_unlock(&fil_system->lock);
            mcontext_free(fil_system->mem_context, (void *)node);
            node = NULL;
        };
    }

    return node;
}

bool32 fil_node_destroy(fil_space_t *space, fil_node_t *node)
{
    fil_node_close(node);

    fil_page_t *page, *tmp;
    spin_lock(&space->lock, NULL);
    UT_LIST_ADD_LAST(chain_list_node, space->fil_nodes, node);
    //
    page = UT_LIST_GET_FIRST(space->free_pages);
    while (page) {
        tmp = UT_LIST_GET_NEXT(list_node, page);
        if (page->file == node->id) {
            UT_LIST_REMOVE(list_node, space->free_pages, page);
        }
        page = tmp;
    }
    spin_unlock(&space->lock);

    spin_lock(&fil_system->lock, NULL);
    fil_system->fil_nodes[node->id] = NULL;
    fil_system->fil_node_num--;
    spin_unlock(&fil_system->lock);

    os_del_file(node->name);

    mcontext_free(fil_system->mem_context, (void *)node);
}

bool32 fil_node_open(fil_space_t *space, fil_node_t *node)
{
    bool32 ret;
    os_file_t handle;
    fil_node_t *last_node;

    if (node->is_open) {
        return TRUE;
    }

    /* File is closed */
    /* If too many files are open, close one */
    spin_lock(&fil_system->lock, NULL);
    while (fil_system->open_pending_num + UT_LIST_GET_LEN(fil_system->fil_node_lru) + 1 >= fil_system->max_n_open) {
        last_node = UT_LIST_GET_LAST(fil_system->fil_node_lru);
        if (last_node == NULL) {
            fprintf(stderr,
                    "Error: cannot close any file to open another for i/o\n"
                    "Pending i/o's on %lu files exist\n",
                    fil_system->open_pending_num);
            ut_a(0);
        }
        handle = last_node->handle;
        last_node->handle = OS_FILE_INVALID_HANDLE;
        UT_LIST_REMOVE(lru_list_node, fil_system->fil_node_lru, node);
        node->is_open = FALSE;
        spin_unlock(&fil_system->lock);

        if (!os_close_file(handle)) {
            fprintf(stderr,
                    "Error: cannot close any file to open another for i/o\n"
                    "Pending i/o's on %lu files exist\n",
                    fil_system->open_pending_num);
            ut_a(0);
        }

        spin_lock(&fil_system->lock, NULL);
    }
    spin_unlock(&fil_system->lock);

    if (!os_open_file(node->name, 0, 0, &node->handle)) {
        return FALSE;
    }

    spin_lock(&fil_system->lock, NULL);
    node->is_open = TRUE;
    UT_LIST_ADD_LAST(lru_list_node, fil_system->fil_node_lru, node);
    spin_unlock(&fil_system->lock);

    return TRUE;
}

bool32 fil_node_close(fil_node_t *node)
{
    bool32 ret;

    ut_a(node->is_open);
    ut_a(node->n_pending == 0);

    ret = os_close_file(node->handle);
    ut_a(ret);

    /* The node is in the LRU list, remove it */
    spin_lock(&fil_system->lock, NULL);
    if (node->is_open) {
        UT_LIST_REMOVE(lru_list_node, fil_system->fil_node_lru, node);
        node->is_open = FALSE;
    }
    spin_unlock(&fil_system->lock);
}


bool32 fil_space_extend_datafile(fil_space_t *space, bool32 need_redo)
{
    fil_node_t *node;
    uint32 total_count = 100;
    uint32 count = 0;

retry:

    if (total_count == 0) {
        return TRUE;
    }

    spin_lock(&space->lock, NULL);
    node = UT_LIST_GET_FIRST(space->fil_nodes);
    while (node) {
        if (node->page_hwm < node->page_max_count) {
            if (node->page_max_count - node->page_hwm >= total_count) {
                node->page_hwm += total_count;
                count = total_count;
            } else {
                node->page_hwm = node->page_max_count;
                total_count -= node->page_max_count - node->page_hwm;
                count = node->page_max_count - node->page_hwm;
            }
            break;
        }
        node = UT_LIST_GET_NEXT(chain_list_node, node);
    }
    spin_unlock(&space->lock);

    if (count > 0 && node) {
        
    }
}


typedef struct st_fil_addr {
    uint32      page;  /* page number within a space */
    uint32      boffset;  /* byte offset within the page */
} fil_addr_t;

/**************************************************************************
Allocates a single free page from a space. The page is marked as used. */
/* out: the page offset, FIL_NULL if no page could be allocated */
static uint32 fsp_alloc_free_page(
    uint32 space,   /* in: space id */
    uint32 hint,    /* in: hint of which page would be desirable */
    mtr_t *mtr)     /* in: mtr handle */
{
    fsp_header_t* header;
    fil_addr_t first;
    xdes_t* descr;
    page_t* page;
    uint32 free;
    uint32 frag_n_used;
    uint32 page_no;

    ut_ad(mtr);

    header = fsp_get_space_header(space, mtr);

    /* Get the hinted descriptor */
    descr = xdes_get_descriptor_with_space_hdr(header, space, hint, mtr);

    if (descr && (xdes_get_state(descr, mtr) == XDES_FREE_FRAG)) {
        /* Ok, we can take this extent */
    } else {
        /* Else take the first extent in free_frag list */
        first = flst_get_first(header + FSP_FREE_FRAG, mtr);

        if (fil_addr_is_null(first)) {
            /* There are no partially full fragments: allocate a free extent and add it to the FREE_FRAG list.
               NOTE that the allocation may have as a side-effect
               that an extent containing a descriptor page is added to the FREE_FRAG list. 
               But we will allocate our page from the the free extent anyway. */
            descr = fsp_alloc_free_extent(space, hint, mtr);
            if (descr == NULL) {
                /* No free space left */
                return(FIL_NULL);
            }
            xdes_set_state(descr, XDES_FREE_FRAG, mtr);
            flst_add_last(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
        } else {
            descr = xdes_lst_get_descriptor(space, first, mtr);
        }

        /* Reset the hint */
        hint = 0;
    }

    /* Now we have in descr an extent with at least one free page. Look for a free page in the extent. */
    free = xdes_find_bit(descr, XDES_FREE_BIT, TRUE, hint % FSP_EXTENT_SIZE, mtr);
    ut_a(free != uint32_UNDEFINED);
    xdes_set_bit(descr, XDES_FREE_BIT, free, FALSE, mtr);

    /* Update the FRAG_N_USED field */
    frag_n_used = mtr_read_uint32(header + FSP_FRAG_N_USED, MLOG_4BYTES, mtr);
    frag_n_used++;
    mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used, MLOG_4BYTES, mtr);
    if (xdes_is_full(descr, mtr)) {
        /* The fragment is full: move it to another list */
        flst_remove(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
        xdes_set_state(descr, XDES_FULL_FRAG, mtr);
        flst_add_last(header + FSP_FULL_FRAG, descr + XDES_FLST_NODE, mtr);
        mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used - FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);
    }

    page_no = xdes_get_offset(descr) + free;

    /* Initialize the allocated page to the buffer pool,
       so that it can be obtained immediately with buf_page_get without need for a disk read. */
    buf_page_create(space, page_no, mtr);
    page = buf_page_get(space, page_no, RW_X_LATCH, mtr);	
    buf_page_dbg_add_level(page, SYNC_FSP_PAGE);

    /* Prior contents of the page should be ignored */
    fsp_init_file_page(page, mtr);

    return page_no;
}

/**************************************************************************
Frees a single page of a space. The page is marked as free and clean. */
static void fsp_free_page(
    uint32 space,   /* in: space id */
    uint32 page,    /* in: page offset */
    mtr_t* mtr)     /* in: mtr handle */
{
    fsp_header_t* header;
    xdes_t* descr;
    uint32 state;
    uint32 frag_n_used;

    ut_ad(mtr);

    /* printf("Freeing page %lu in space %lu\n", page, space); */

    header = fsp_get_space_header(space, mtr);
    descr = xdes_get_descriptor_with_space_hdr(header, space, page, mtr);
    state = xdes_get_state(descr, mtr);

    ut_a((state == XDES_FREE_FRAG) || (state == XDES_FULL_FRAG));
    ut_a(xdes_get_bit(descr, XDES_FREE_BIT, page % FSP_EXTENT_SIZE, mtr) == FALSE);

    xdes_set_bit(descr, XDES_FREE_BIT, page % FSP_EXTENT_SIZE, TRUE, mtr);
    xdes_set_bit(descr, XDES_CLEAN_BIT, page % FSP_EXTENT_SIZE, TRUE, mtr);

    frag_n_used = mtr_read_uint32(header + FSP_FRAG_N_USED, MLOG_4BYTES, mtr);
    if (state == XDES_FULL_FRAG) {
        /* The fragment was full: move it to another list */
        flst_remove(header + FSP_FULL_FRAG, descr + XDES_FLST_NODE, mtr);
        xdes_set_state(descr, XDES_FREE_FRAG, mtr);
        flst_add_last(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
        mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used + FSP_EXTENT_SIZE - 1, MLOG_4BYTES, mtr);
    } else {
        ut_a(frag_n_used > 0);
        mlog_write_uint32(header + FSP_FRAG_N_USED, frag_n_used - 1, MLOG_4BYTES, mtr);
    }

    if (xdes_is_free(descr, mtr)) {
        /* The extent has become free: move it to another list */
        flst_remove(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
        fsp_free_extent(space, page, mtr);
    }
}

/* Allocates a new free extent. */
/* out: extent descriptor, NULL if cannot be allocated */
static xdes_t* fsp_alloc_free_extent(
    uint32 space,   /* in: space id */
    uint32 hint,    /* in: hint of which extent would be desirable: 
                           any page offset in the extent goes; the hint must not be > FSP_FREE_LIMIT */
    mtr_t* mtr)     /* in: mtr */
{
    fsp_header_t* header;
    fil_addr_t first;
    xdes_t* descr;

    ut_ad(mtr);

    header = fsp_get_space_header(space, mtr);
    descr = xdes_get_descriptor_with_space_hdr(header, space, hint, mtr);

    if (descr && (xdes_get_state(descr, mtr) == XDES_FREE)) {
        /* Ok, we can take this extent */
    } else {
        /* Take the first extent in the free list */
        first = flst_get_first(header + FSP_FREE, mtr);
        if (fil_addr_is_null(first)) {
            fsp_fill_free_list(space, header, mtr);
            first = flst_get_first(header + FSP_FREE, mtr);
        }
        if (fil_addr_is_null(first)) {
            return(NULL); /* No free extents left */
        }
        descr = xdes_lst_get_descriptor(space, first, mtr);
    }
    flst_remove(header + FSP_FREE, descr + XDES_FLST_NODE, mtr);

    return(descr);
}


/**************************************************************************
Returns an extent to the free list of a space. */
static void fsp_free_extent(
    uint32 space,  /* in: space id */
    uint32 page,   /* in: page offset in the extent */
    mtr_t* mtr)    /* in: mtr */
{
    fsp_header_t* header;
    xdes_t* descr;

    ut_ad(mtr);

    header = fsp_get_space_header(space, mtr);
    descr = xdes_get_descriptor_with_space_hdr(header, space, page, mtr);
    ut_a(xdes_get_state(descr, mtr) != XDES_FREE);
    xdes_init(descr, mtr);
    flst_add_last(header + FSP_FREE, descr + XDES_FLST_NODE, mtr);
}

/** Check if tablespace is dd tablespace.
@param[in]      space_id        tablespace ID
@return true if tablespace is dd tablespace. */
bool32 fsp_is_dd_tablespace(space_id_t space_id)
{
    return (space_id == dict_sys_t::s_space_id);
}

/** Check whether a space id is an undo tablespace ID
Undo tablespaces have space_id's starting 1 less than the redo logs.
They are numbered down from this.  Since rseg_id=0 always refers to the
system tablespace, undo_space_num values start at 1.  The current limit
is 127. The translation from an undo_space_num is:
   undo space_id = log_first_space_id - undo_space_num
@param[in]	space_id	space id to check
@return true if it is undo tablespace else false. */
bool32 fsp_is_undo_tablespace(space_id_t space_id)
{
  /* Starting with v8, undo space_ids have a unique range. */
  if (space_id >= dict_sys_t::s_min_undo_space_id &&
      space_id <= dict_sys_t::s_max_undo_space_id) {
    return (true);
  }

  /* If upgrading from 5.7, there may be a list of old-style
  undo tablespaces.  Search them. */
  if (trx_sys_undo_spaces != nullptr) {
    return (trx_sys_undo_spaces->contains(space_id));
  }

  return (false);
}

/** Check if tablespace is global temporary.
@param[in]	space_id	tablespace ID
@return true if tablespace is global temporary. */
bool32 fsp_is_global_temporary(space_id_t space_id)
{
    return (space_id == srv_tmp_space.space_id());
}

/** Check if the tablespace is session temporary.
@param[in]      space_id        tablespace ID
@return true if tablespace is a session temporary tablespace. */
bool32 fsp_is_session_temporary(space_id_t space_id)
{
    return (space_id > dict_sys_t::s_min_temp_space_id &&
            space_id <= dict_sys_t::s_max_temp_space_id);
}

/** Check if tablespace is system temporary.
@param[in]	space_id	tablespace ID
@return true if tablespace is system temporary. */
bool32 fsp_is_system_temporary(space_id_t space_id)
{
    return (fsp_is_global_temporary(space_id) ||
            fsp_is_session_temporary(space_id));
}

