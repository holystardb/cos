#ifndef _VIO_PAT_H
#define _VIO_PAT_H

#include "cm_type.h"
#include "cm_memory.h"

#ifdef __cplusplus
extern "C" {
#endif

#define EVENT_INIT           0
#define EVENT_TIMER1         1
#define EVENT_TIMER2         2
#define EVENT_TIMER3         3
#define EVENT_TIMER4         4
#define EVENT_TIMER5         5
#define EVENT_TIMER6         6
#define EVENT_TIMER7         7
#define EVENT_TIMER8         8
#define EVENT_TIMER9         9
#define EVENT_DEL_TIMER      10
#define EVENT_RECV           100
#define EVENT_USER           0xFF

#define INVALID_PNO          0xFF
#define INVALID_EVENT        0xFFFF
#define INVALID_TIMER        0xFFFFFFFF

typedef int32 (*pat_callback_func)(uint8 sender, uint16 event, void* data, uint32 len);

bool32 pat_pool_init(uint8 reactor_count, memory_pool_t *mpool);
void pat_pool_destroy();
bool32 pat_pool_set_auth(char *user, char* password);

bool32 pat_startup_as_server(uint8 pno, pat_callback_func func_ptr, char *host, uint16 port);
bool32 pat_startup_as_client(uint8 pno, pat_callback_func func_ptr);
bool32 pat_set_server_info(uint8 pno, char *host, uint16 port);

bool32 pat_append_to_list(uint8 dest_pno, uint8 src_pno, uint16 event, void* data, uint32 len);
bool32 pat_send_data(uint8 receiver, uint8 sender, char* data, uint32 len);

uint8 CURR_PNO();
uint16 CURR_EVENT();

uint32 pat_set_timer(uint8 pno, uint16 event, uint32 count100ms, void* arg);
bool32 pat_del_timer(uint8 pno, uint32 timer_id);



#ifdef __cplusplus
}
#endif

#endif  /* _VIO_PAT_H */

