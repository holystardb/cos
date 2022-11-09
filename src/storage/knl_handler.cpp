#include "knl_handler.h"
#include "knl_server.h"


bool32 knl_server_init(void)
{
    dberr_t err;

    err = srv_start(TRUE);

    return DB_SUCCESS;
}


