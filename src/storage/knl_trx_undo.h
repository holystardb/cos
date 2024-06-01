#ifndef _KNL_TRX_UNDO_H
#define _KNL_TRX_UNDO_H

#include "cm_type.h"
#include "knl_dict.h"



// Transaction undo log memory object;
// this is protected by the undo_mutex in the corresponding transaction object
struct trx_undo_t{
	uint32		id;		/*!< undo log slot number within the
					rollback segment */
	uint32		type;		/*!< TRX_UNDO_INSERT or
					TRX_UNDO_UPDATE */
	uint32		state;		/*!< state of the corresponding undo log
					segment */
	bool32		del_marks;	/*!< relevant only in an update undo
					log: this is TRUE if the transaction may
					have delete marked records, because of
					a delete of a row or an update of an
					indexed field; purge is then
					necessary; also TRUE if the transaction
					has updated an externally stored
					field */
	trx_id_t	trx_id;		/*!< id of the trx assigned to the undo
					log */
	//XID		xid;		/*!< X/Open XA transaction identification */
	bool32		dict_operation;	/*!< TRUE if a dict operation trx */
	table_id_t	table_id;	/*!< if a dict operation, then the table
					id */
	trx_rseg_t*	rseg;		/*!< rseg where the undo log belongs */
	/*-----------------------------*/
	uint32		space;		/*!< space id where the undo log
					placed */
	uint32		zip_size;	/*!< compressed page size of space
					in bytes, or 0 for uncompressed */
	uint32		hdr_page_no;	/*!< page number of the header page in
					the undo log */
	uint32		hdr_offset;	/*!< header offset of the undo log on
				       	the page */
	uint32		last_page_no;	/*!< page number of the last page in the
					undo log; this may differ from
					top_page_no during a rollback */
	uint32		size;		/*!< current size in pages */
	/*-----------------------------*/
	uint32		empty;		/*!< TRUE if the stack of undo log
					records is currently empty */
	uint32		top_page_no;	/*!< page number where the latest undo
					log record was catenated; during
					rollback the page from which the latest
					undo record was chosen */
	uint32		top_offset;	/*!< offset of the latest undo record,
					i.e., the topmost element in the undo
					log if we think of it as a stack */
	uint64      top_undo_no;	/*!< undo number of the latest record */
	buf_block_t*	guess_block;	/*!< guess for the buffer block where
					the top page might reside */
	/*-----------------------------*/
	UT_LIST_NODE_T(trx_undo_t) undo_list;
					/*!< undo log objects in the rollback
					segment are chained into lists */
};


//-----------------------------------------------------------------



#endif  /* _KNL_TRX_UNDO_H */
