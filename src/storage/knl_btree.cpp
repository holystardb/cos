#include "knl_btree.h"




// Create the root node for a new index tree.
uint32 btr_create(
    uint32			type,
    uint32			space,
    const page_size_t&	page_size,
    index_id_t		index_id,
    dict_index_t*		index,
    const btr_create_t*	btr_redo_create_info,
    mtr_t*			mtr)
{
    return 0;
}

