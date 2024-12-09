#ifndef _KNL_START_H
#define _KNL_START_H

#include "cm_type.h"
#include "cm_memory.h"
#include "cm_attribute.h"
#include "knl_session.h"
#include "knl_heap.h"


extern status_t server_open_or_create_database(char* base_dir, attribute_t* attr);
extern status_t server_shutdown_database();

#endif  /* _KNL_START_H */
