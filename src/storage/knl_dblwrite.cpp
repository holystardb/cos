#include "knl_dblwrite.h"

bool32    buf_dblwr_being_created = FALSE;


// Determines if a page number is located inside the doublewrite buffer.
// return TRUE if the location is inside the two blocks of the doublewrite buffer
bool32 buf_dblwr_page_inside(const page_id_t &page_id)
{
#if 0
	if (buf_dblwr == NULL) {

		return(FALSE);
	}

	if (page_no >= buf_dblwr->block1
	    && page_no < buf_dblwr->block1
	    + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) {
		return(TRUE);
	}

	if (page_no >= buf_dblwr->block2
	    && page_no < buf_dblwr->block2
	    + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) {
		return(TRUE);
	}
#endif
    return(FALSE);
}

