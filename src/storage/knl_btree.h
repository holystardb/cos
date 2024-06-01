#ifndef _KNL_BTREE_H
#define _KNL_BTREE_H

#include "cm_type.h"
#include "knl_dict.h"
#include "knl_mtr.h"
#include "knl_page_id.h"
#include "knl_page_size.h"

/** Persistent cursor */
struct btr_pcur_t;
/** B-tree cursor */
struct btr_cur_t;
/** B-tree search information for the adaptive hash index */
struct btr_search_t;

/** Is search system enabled.
Search system is protected by array of latches. */
extern char	btr_search_enabled;

/** Number of adaptive hash index partition. */
extern ulong	btr_ahi_parts;

/** The size of a reference to data stored on a different page.
The reference is stored at the end of the prefix of the field
in the index record. */
#define BTR_EXTERN_FIELD_REF_SIZE	FIELD_REF_SIZE

/** If the data don't exceed the size, the data are stored locally. */
#define BTR_EXTERN_LOCAL_STORED_MAX_SIZE	\
	(BTR_EXTERN_FIELD_REF_SIZE * 2)

/** The information is used for creating a new index tree when
applying TRUNCATE log record during recovery */
struct btr_create_t {

    explicit btr_create_t(const byte* const ptr)
        :
        format_flags(),
        n_fields(),
        field_len(),
        fields(ptr),
        trx_id_pos(UINT32_UNDEFINED)
    {
        /* Do nothing */
    }

    /** Page format */
    uint32			format_flags;

    /** Numbr of index fields */
    uint32			n_fields;

    /** The length of the encoded meta-data */
    uint32			field_len;

    /** Field meta-data, encoded. */
    const byte* const	fields;

    /** Position of trx-id column. */
    uint32			trx_id_pos;
};

uint32 btr_create(
    uint32			type,
    uint32			space,
    const page_size_t&	page_size,
    index_id_t		index_id,
    dict_index_t*		index,
    const btr_create_t*	btr_redo_create_info,
    mtr_t*			mtr);

#endif  /* _KNL_BTREE_H */
