#ifndef _KNL_TRX_H
#define _KNL_TRX_H

#include "cm_type.h"
#include "knl_dict.h"

/** Transaction execution states when trx->state == TRX_STATE_ACTIVE */
enum trx_que_t {
	TRX_QUE_RUNNING,		/*!< transaction is running */
	TRX_QUE_LOCK_WAIT,		/*!< transaction is waiting for
					a lock */
	TRX_QUE_ROLLING_BACK,		/*!< transaction is rolling back */
	TRX_QUE_COMMITTING		/*!< transaction is committing */
};

/** Transaction states (trx_t::state) */
enum trx_state_t {
	TRX_STATE_NOT_STARTED,
	TRX_STATE_ACTIVE,
	TRX_STATE_PREPARED,			/* Support for 2PC/XA */
	TRX_STATE_COMMITTED_IN_MEMORY
};


/** The locks and state of an active transaction. Protected by
lock_sys->mutex, trx->mutex or both. */
struct trx_lock_t {
	uint32		n_active_thrs;	/*!< number of active query threads */

	trx_que_t	que_state;	/*!< valid when trx->state
					== TRX_STATE_ACTIVE: TRX_QUE_RUNNING,
					TRX_QUE_LOCK_WAIT, ... */

	//lock_t*		wait_lock;	
                /*!< if trx execution state is
					TRX_QUE_LOCK_WAIT, this points to
					the lock request, otherwise this is
					NULL; set to non-NULL when holding
					both trx->mutex and lock_sys->mutex;
					set to NULL when holding
					lock_sys->mutex; readers should
					hold lock_sys->mutex, except when
					they are holding trx->mutex and
					wait_lock==NULL */
	uint64	deadlock_mark;	/*!< A mark field that is initialized
					to and checked against lock_mark_counter
					by lock_deadlock_recursive(). */
	bool32		was_chosen_as_deadlock_victim;
					/*!< when the transaction decides to
					wait for a lock, it sets this to FALSE;
					if another transaction chooses this
					transaction as a victim in deadlock
					resolution, it sets this to TRUE.
					Protected by trx->mutex. */
	time_t		wait_started;	/*!< lock wait started at this time,
					protected only by lock_sys->mutex */

	//que_thr_t*	wait_thr;	
                /*!< query thread belonging to this
					trx that is in QUE_THR_LOCK_WAIT
					state. For threads suspended in a
					lock wait, this is protected by
					lock_sys->mutex. Otherwise, this may
					only be modified by the thread that is
					serving the running transaction. */

	//mem_heap_t*	lock_heap;	/*!< memory heap for trx_locks;
	//				protected by lock_sys->mutex */

	//UT_LIST_BASE_NODE_T(lock_t) trx_locks;	
            /*!< locks requested
					by the transaction;
					insertions are protected by trx->mutex
					and lock_sys->mutex; removals are
					protected by lock_sys->mutex */

	//ib_vector_t*	table_locks;	/*!< All table locks requested by this
	//				transaction, including AUTOINC locks */

	bool32		cancel;		/*!< TRUE if the transaction is being
					rolled back either via deadlock
					detection or due to lock timeout. The
					caller has to acquire the trx_t::mutex
					in order to cancel the locks. In
					lock_trx_table_locks_remove() we
					check for this cancel of a transaction's
					locks and avoid reacquiring the trx
					mutex to prevent recursive deadlocks.
					Protected by both the lock sys mutex
					and the trx_t::mutex. */
};

#define TRX_MAGIC_N	91118598

struct trx_t{
	uint32		magic_n;

	mutex_t	mutex;		/*!< Mutex protecting the fields
					state and lock
					(except some fields of lock, which
					are protected by lock_sys->mutex) */

	/** State of the trx from the point of view of concurrency control
	and the valid state transitions.

	Possible states:

	TRX_STATE_NOT_STARTED
	TRX_STATE_ACTIVE
	TRX_STATE_PREPARED
	TRX_STATE_COMMITTED_IN_MEMORY (alias below COMMITTED)

	Valid state transitions are:

	Regular transactions:
	* NOT_STARTED -> ACTIVE -> COMMITTED -> NOT_STARTED

	Auto-commit non-locking read-only:
	* NOT_STARTED -> ACTIVE -> NOT_STARTED

	XA (2PC):
	* NOT_STARTED -> ACTIVE -> PREPARED -> COMMITTED -> NOT_STARTED

	Recovered XA:
	* NOT_STARTED -> PREPARED -> COMMITTED -> (freed)

	XA (2PC) (shutdown before ROLLBACK or COMMIT):
	* NOT_STARTED -> PREPARED -> (freed)

	Latching and various transaction lists membership rules:

	XA (2PC) transactions are always treated as non-autocommit.

	Transitions to ACTIVE or NOT_STARTED occur when
	!in_rw_trx_list and !in_ro_trx_list (no trx_sys->mutex needed).

	Autocommit non-locking read-only transactions move between states
	without holding any mutex. They are !in_rw_trx_list, !in_ro_trx_list.

	When a transaction is NOT_STARTED, it can be in_mysql_trx_list if
	it is a user transaction. It cannot be in ro_trx_list or rw_trx_list.

	ACTIVE->PREPARED->COMMITTED is only possible when trx->in_rw_trx_list.
	The transition ACTIVE->PREPARED is protected by trx_sys->mutex.

	ACTIVE->COMMITTED is possible when the transaction is in
	ro_trx_list or rw_trx_list.

	Transitions to COMMITTED are protected by both lock_sys->mutex
	and trx->mutex.

	NOTE: Some of these state change constraints are an overkill,
	currently only required for a consistent view for printing stats.
	This unnecessarily adds a huge cost for the general case.

	NOTE: In the future we should add read only transactions to the
	ro_trx_list the first time they try to acquire a lock ie. by default
	we treat all read-only transactions as non-locking.  */
	trx_state_t	state;

	trx_lock_t	lock;		/*!< Information about the transaction
					locks and state. Protected by
					trx->mutex or lock_sys->mutex
					or both */
	uint32		is_recovered;	/*!< 0=normal transaction,
					1=recovered, must be rolled back,
					protected by trx_sys->mutex when
					trx->in_rw_trx_list holds */

	/* These fields are not protected by any mutex. */
	const char*	op_info;	/*!< English text describing the
					current operation, or an empty
					string */
	uint32		isolation_level;/*!< TRX_ISO_REPEATABLE_READ, ... */
	uint32		check_foreigns;	/*!< normally TRUE, but if the user
					wants to suppress foreign key checks,
					(in table imports, for example) we
					set this FALSE */
	/*------------------------------*/
	/* MySQL has a transaction coordinator to coordinate two phase
	commit between multiple storage engines and the binary log. When
	an engine participates in a transaction, it's responsible for
	registering itself using the trans_register_ha() API. */
	unsigned	is_registered:1;/* This flag is set to 1 after the
					transaction has been registered with
					the coordinator using the XA API, and
					is set to 0 after commit or rollback. */
	unsigned	owns_prepare_mutex:1;/* 1 if owns prepare mutex, if
					this is set to 1 then registered should
					also be set to 1. This is used in the
					XA code */
	/*------------------------------*/
	uint32		check_unique_secondary;
					/*!< normally TRUE, but if the user
					wants to speed up inserts by
					suppressing unique key checks
					for secondary indexes when we decide
					if we can use the insert buffer for
					them, we set this FALSE */
	uint32		support_xa;	/*!< normally we do the XA two-phase
					commit steps, but by setting this to
					FALSE, one can save CPU time and about
					150 bytes in the undo log size as then
					we skip XA steps */
	uint32		flush_log_later;/* In 2PC, we hold the
					prepare_commit mutex across
					both phases. In that case, we
					defer flush of the logs to disk
					until after we release the
					mutex. */
	uint32		must_flush_log_later;/*!< this flag is set to TRUE in
					trx_commit() if flush_log_later was
					TRUE, and there were modifications by
					the transaction; in that case we must
					flush the log in
					trx_commit_complete_for_mysql() */
	uint32		duplicates;	/*!< TRX_DUP_IGNORE | TRX_DUP_REPLACE */
	uint32		has_search_latch;
					/*!< TRUE if this trx has latched the
					search system latch in S-mode */
	uint32		search_latch_timeout;
					/*!< If we notice that someone is
					waiting for our S-lock on the search
					latch to be released, we wait in
					row0sel.cc for BTR_SEA_TIMEOUT new
					searches until we try to keep
					the search latch again over
					calls from MySQL; this is intended
					to reduce contention on the search
					latch */
	//trx_dict_op_t	dict_operation;	/**< @see enum trx_dict_op */

	/* Fields protected by the srv_conc_mutex. */
	uint32		declared_to_be_inside_innodb;
					/*!< this is TRUE if we have declared
					this transaction in
					srv_conc_enter_innodb to be inside the
					InnoDB engine */
	uint32		n_tickets_to_enter_innodb;
					/*!< this can be > 0 only when
					declared_to_... is TRUE; when we come
					to srv_conc_innodb_enter, if the value
					here is > 0, we decrement this by 1 */
	uint32		dict_operation_lock_mode;
					/*!< 0, RW_S_LATCH, or RW_X_LATCH:
					the latch mode trx currently holds
					on dict_operation_lock. Protected
					by dict_operation_lock. */

	trx_id_t	no;		/*!< transaction serialization number:
					max trx id shortly before the
					transaction is moved to
					COMMITTED_IN_MEMORY state.
					Protected by trx_sys_t::mutex
					when trx->in_rw_trx_list. Initially
					set to TRX_ID_MAX. */

	time_t		start_time;	/*!< time the trx object was created
					or the state last time became
					TRX_STATE_ACTIVE */
	trx_id_t	id;		/*!< transaction id */
	//XID		xid;		/*!< X/Open XA transaction
	//				identification to identify a
	//				transaction branch */
	lsn_t		commit_lsn;	/*!< lsn at the time of the commit */
	table_id_t	table_id;	/*!< Table to drop iff dict_operation
					== TRX_DICT_OP_TABLE, or 0. */
	/*------------------------------*/
	//THD*		mysql_thd;	/*!< MySQL thread handle corresponding
	//				to this trx, or NULL */
	const char*	mysql_log_file_name;
					/*!< if MySQL binlog is used, this field
					contains a pointer to the latest file
					name; this is NULL if binlog is not
					used */
	int64	mysql_log_offset;
					/*!< if MySQL binlog is used, this
					field contains the end offset of the
					binlog entry */
	/*------------------------------*/
	uint32		n_mysql_tables_in_use; /*!< number of Innobase tables
					used in the processing of the current
					SQL statement in MySQL */
	uint32		mysql_n_tables_locked;
					/*!< how many tables the current SQL
					statement uses, except those
					in consistent read */
	/*------------------------------*/
	UT_LIST_NODE_T(trx_t)
			trx_list;	/*!< list of transactions;
					protected by trx_sys->mutex.
					The same node is used for both
					trx_sys_t::ro_trx_list and
					trx_sys_t::rw_trx_list */
#ifdef UNIV_DEBUG
	/** The following two fields are mutually exclusive. */
	/* @{ */

	bool32		in_ro_trx_list;	/*!< TRUE if in trx_sys->ro_trx_list */
	bool32		in_rw_trx_list;	/*!< TRUE if in trx_sys->rw_trx_list */
	/* @} */
#endif /* UNIV_DEBUG */
	UT_LIST_NODE_T(trx_t)
			mysql_trx_list;	/*!< list of transactions created for
					MySQL; protected by trx_sys->mutex */
#ifdef UNIV_DEBUG
	bool32		in_mysql_trx_list;
					/*!< TRUE if in
					trx_sys->mysql_trx_list */
#endif /* UNIV_DEBUG */
	/*------------------------------*/
	dberr_t		error_state;	/*!< 0 if no error, otherwise error
					number; NOTE That ONLY the thread
					doing the transaction is allowed to
					set this field: this is NOT protected
					by any mutex */
	//const dict_index_t *error_info;	/*!< if the error number indicates a
	//				duplicate key error, a pointer to
	//				the problematic index is stored here */
	uint32		error_key_num;	/*!< if the index creation fails to a
					duplicate key error, a mysql key
					number of that index is stored here */
	//sess_t*		sess;		/*!< session of the trx, NULL if none */
	//que_t*		graph;		
                /*!< query currently run in the session,
					or NULL if none; NOTE that the query
					belongs to the session, and it can
					survive over a transaction commit, if
					it is a stored procedure with a COMMIT
					WORK statement, for instance */
	//mem_heap_t*	global_read_view_heap;
					/*!< memory heap for the global read
					view */
	//read_view_t*	global_read_view;
					/*!< consistent read view associated
					to a transaction or NULL */
	//read_view_t*	read_view;
            	/*!< consistent read view used in the
					transaction or NULL, this read view
					if defined can be normal read view
					associated to a transaction (i.e.
					same as global_read_view) or read view
					associated to a cursor */
	/*------------------------------*/
	//UT_LIST_BASE_NODE_T(trx_named_savept_t)
	//		trx_savepoints;	/*!< savepoints set with SAVEPOINT ..., oldest first */
	/*------------------------------*/
	mutex_t	undo_mutex;	/*!< mutex protecting the fields in this
					section (down to undo_no_arr), EXCEPT
					last_sql_stat_start, which can be
					accessed only when we know that there
					cannot be any activity in the undo
					logs! */
	//undo_no_t	undo_no;
        	/*!< next undo log record number to
					assign; since the undo log is
					private for a transaction, this
					is a simple ascending sequence
					with no gaps; thus it represents
					the number of modified/inserted
					rows in a transaction */
	//trx_savept_t	last_sql_stat_start;
					/*!< undo_no when the last sql statement
					was started: in case of an error, trx
					is rolled back down to this undo
					number; see note at undo_mutex! */
	//trx_rseg_t*	rseg;
            		/*!< rollback segment assigned to the
					transaction, or NULL if not assigned
					yet */
	//trx_undo_t*	insert_undo;	/*!< pointer to the insert undo log, or NULL if no inserts performed yet */
	//trx_undo_t*	update_undo;	/*!< pointer to the update undo log, or NULL if no update performed yet */
	//undo_no_t	roll_limit;	/*!< least undo number to undo during a rollback */
	uint32		pages_undone;	/*!< number of undo log pages undone
					since the last undo log truncation */
	//trx_undo_arr_t*	undo_no_arr;	
            /*!< array of undo numbers of undo log
					records which are currently processed
					by a rollback operation */
	/*------------------------------*/
	uint32		n_autoinc_rows;	/*!< no. of AUTO-INC rows required for
					an SQL statement. This is useful for
					multi-row INSERTs */
	//ib_vector_t*    autoinc_locks;
              /* AUTOINC locks held by this
					transaction. Note that these are
					also in the lock list trx_locks. This
					vector needs to be freed explicitly
					when the trx instance is destroyed.
					Protected by lock_sys->mutex. */
	/*------------------------------*/
	bool32		read_only;	/*!< TRUE if transaction is flagged
					as a READ-ONLY transaction.
					if !auto_commit || will_lock > 0
					then it will added to the list
					trx_sys_t::ro_trx_list. A read only
					transaction will not be assigned an
					UNDO log. Non-locking auto-commit
					read-only transaction will not be on
					either list. */
	bool32		auto_commit;	/*!< TRUE if it is an autocommit */
	uint32		will_lock;	/*!< Will acquire some locks. Increment
					each time we determine that a lock will
					be acquired by the MySQL layer. */
	bool32		ddl;		/*!< true if it is a transaction that
					is being started for a DDL operation */
	/*------------------------------*/
	//fts_trx_t*	fts_trx;	/*!< FTS information, or NULL if
	//				transaction hasn't modified tables
	//				with FTS indexes (yet). */
	//doc_id_t	fts_next_doc_id;/* The document id used for updates */
	/*------------------------------*/
	uint32		flush_tables;	/*!< if "covering" the FLUSH TABLES",
					count of tables being flushed. */

	/*------------------------------*/
#ifdef UNIV_DEBUG
	uint32		start_line;	/*!< Track where it was started from */
	const char*	start_file;	/*!< Filename where it was started */
#endif /* UNIV_DEBUG */

	/*------------------------------*/
	char detailed_error[256];	/*!< detailed error message for last
					error, or empty. */
};




#endif  /* _KNL_TRX_H */
