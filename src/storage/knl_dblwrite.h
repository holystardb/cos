#ifndef _KNL_DOUBLE_WRITE_H
#define _KNL_DOUBLE_WRITE_H

#include "cm_type.h"
#include "knl_page_id.h"


extern bool32 buf_dblwr_page_inside(const page_id_t &page_id);

extern bool32    buf_dblwr_being_created;


#endif  /* _KNL_DOUBLE_WRITE_H */
