#ifndef _KNL_TRX_RSEG_H
#define _KNL_TRX_RSEG_H

#include "cm_type.h"
#include "cm_list.h"
#include "knl_page_size.h"
#include "knl_flst.h"
#include "knl_mtr.h"
#include "knl_trx_undo.h"
#include "knl_trx_types.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

extern status_t trx_sysf_create();
extern inline trx_sysf_t* trx_sysf_get(mtr_t* mtr);


extern status_t trx_rseg_open_trx_slots(trx_rseg_t* rseg);
extern status_t trx_rseg_create_trx_slots(trx_rseg_t* rseg);
extern status_t trx_rseg_undo_page_init(trx_rseg_t* rseg);

extern inline trx_undo_page_t* trx_rseg_alloc_free_undo_page(trx_rseg_t* rseg);
extern inline void trx_rseg_free_free_undo_page(trx_rseg_t* rseg, trx_undo_page_t* undo_page);

extern inline trx_t* trx_rseg_assign_and_alloc_trx();
extern inline void trx_rseg_release_trx(trx_t* trx);
extern inline scn_t trx_rseg_set_end(trx_t* trx, bool32 is_commit);
extern inline uint64 trx_get_next_scn();
extern inline void trx_get_status_by_itl(trx_slot_id_t trx_slot_id, trx_status_t* trx_status);

extern byte* trx_rseg_replay_init_page(uint32 type, byte* log_rec_ptr, byte* log_end_ptr, void* block);
extern byte* trx_rseg_replay_begin_slot(uint32 type, byte* log_rec_ptr, byte* log_end_ptr, void* block);
extern byte* trx_rseg_replay_end_slot(uint32 type, byte* log_rec_ptr, byte* log_end_ptr, void* block);
//-----------------------------------------------------------------






#ifdef __cplusplus
}
#endif // __cplusplus

#endif  /* _KNL_TRX_RSEG_H */
