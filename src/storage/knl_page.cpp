#include "knl_page.h"
#include "knl_mtr.h"
#include "knl_fsp.h"



/* The data structures in files are defined just as byte strings in C */
typedef byte    fsp_header_t;
typedef byte    xdes_t;

/* 'null' (undefined) page offset in the context of file spaces */
#define FIL_NULL            0xFFFFFFFF


/* SPACE HEADER
File space header data structure: this data structure is contained in the
first page of a space. The space for this header is reserved in every extent
descriptor page, but used only in the first. */

#define FSP_HEADER_OFFSET       FIL_PAGE_DATA    /* Offset of the space header within a file page */
/*-------------------------------------*/
#define FSP_NOT_USED            0    /* this field contained a value up to
                    which we know that the modifications
                    in the database have been flushed to the file space; not used now */
#define FSP_SIZE                8    /* Current size of the space in pages */
#define FSP_FREE_LIMIT          12    /* Minimum page number for which the
                    free list has not been initialized: the pages >= this limit are, by definition free */
#define FSP_LOWEST_NO_WRITE     16    /* The lowest page offset for which
                    the page has not been written to disk
                    (if it has been written, we know that
                    the OS has really reserved the physical space for the page) */
#define FSP_FRAG_N_USED         20    /* number of used pages in the FSP_FREE_FRAG list */
#define FSP_FREE                24    /* list of free extents */
#define FSP_FREE_FRAG           (24 + FLST_BASE_NODE_SIZE)
                    /* list of partially free extents not belonging to any segment */
#define FSP_FULL_FRAG           (24 + 2 * FLST_BASE_NODE_SIZE)
                    /* list of full extents not belonging to any segment */
#define FSP_SEG_ID              (24 + 3 * FLST_BASE_NODE_SIZE)
                    /* 8 bytes which give the first unused segment id */
#define FSP_SEG_INODES_FULL     (32 + 3 * FLST_BASE_NODE_SIZE)
                    /* list of pages containing segment headers, where all the segment inode slots are reserved */
#define FSP_SEG_INODES_FREE     (32 + 4 * FLST_BASE_NODE_SIZE)
                    /* list of pages containing segment
                    headers, where not all the segment
                    header slots are reserved */
/*-------------------------------------*/
/* File space header size */
#define FSP_HEADER_SIZE         (32 + 5 * FLST_BASE_NODE_SIZE)

#define FSP_FREE_ADD            4    /* this many free extents are added to the free list from above FSP_FREE_LIMIT at a time */


/* File space extent size in pages */
#define FSP_EXTENT_SIZE         64





/* Space address data type; this is intended to be used when addresses accurate to a byte are stored in file pages.
If the page part of the address is FIL_NULL, the address is considered undefined. */

typedef byte    fil_faddr_t;        /* 'type' definition in C: an address stored in a file page is a string of bytes */
#define FIL_ADDR_PAGE   0   /* first in address is the page offset */
#define FIL_ADDR_BYTE   4   /* then comes 2-byte byte offset within page*/
#define FIL_ADDR_SIZE   6   /* address size is 6 bytes */



/* The C 'types' of base node and list node:
these should be used to write self-documenting code. Of course, the sizeof macro cannot be applied to these types! */

typedef byte            flst_base_node_t;
typedef byte            flst_node_t;

/* The physical size of a list base node in bytes */
#define FLST_BASE_NODE_SIZE     (4 + 2 * FIL_ADDR_SIZE)

/* The physical size of a list node in bytes */
#define FLST_NODE_SIZE          (2 * FIL_ADDR_SIZE)


/*      EXTENT DESCRIPTOR
File extent descriptor data structure: contains bits to tell which pages in
the extent are free and which contain old tuple version to clean. */

/*-------------------------------------*/
#define XDES_ID             0   /* The identifier of the segment to which this extent belongs */
#define XDES_FLST_NODE      8   /* The list node data structure for the descriptors */
#define XDES_STATE          (FLST_NODE_SIZE + 8)    /* contains state information of the extent */
#define XDES_BITMAP         (FLST_NODE_SIZE + 12)   /* Descriptor bitmap of the pages in the extent */

/*-------------------------------------*/

#define XDES_BITS_PER_PAGE      2   /* How many bits are there per page */
#define XDES_FREE_BIT           0   /* Index of the bit which tells if the page is free */
#define	XDES_CLEAN_BIT          1   /* NOTE: currently not used!
                                       Index of the bit which tells if there are old versions of tuples on the page */
/* States of a descriptor */
#define XDES_FREE               1   /* extent is in free list of space */
#define XDES_FREE_FRAG          2   /* extent is in free fragment list of space */
#define XDES_FULL_FRAG          3   /* extent is in full fragment list of space */
#define XDES_FSEG               4   /* extent belongs to a segment */

/* File extent data structure size in bytes. The "+ 7 ) / 8" part in the definition rounds the number of bytes upward. */
#define XDES_SIZE               (XDES_BITMAP + (FSP_EXTENT_SIZE * XDES_BITS_PER_PAGE + 7) / 8)

/* Offset of the descriptor array on a descriptor page */
#define XDES_ARR_OFFSET         (FSP_HEADER_OFFSET + FSP_HEADER_SIZE)




