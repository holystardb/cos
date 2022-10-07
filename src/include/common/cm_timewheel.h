#ifndef _CM_TIME_WHEEL_H
#define _CM_TIME_WHEEL_H

#include "cm_type.h"
#include "cm_biqueue.h"


#define BUFFER_SIZE 64

#ifdef __cplusplus
extern "C" {
#endif

#define INVALID_TIMER_ID        0xFFFFFFFF

/* 时间轮上槽的数据 */
#define TIME_WHEEL_N            600

/* 每100ms时间轮转动一次，即槽 间隔为100ms */
#define TIME_WHEEL_SI           1

typedef struct st_tw_timer   tw_timer_t;
typedef void* (*tw_timer_callback_func)(tw_timer_t *timer, void *arg);

/*定时器类*/
struct st_tw_timer
{
    void                   *arg;  /*客户数据*/
    tw_timer_callback_func  func;
    uint16                  event;

    uint32                  rotation;  /*记录定时器在时间轮转多少圈后生效*/
    uint32                  time_slot;  /*记录定时器属于时间轮上的哪个槽(对应的链表，下同)*/
    struct st_tw_timer     *prev;
    struct st_tw_timer     *next;
    union {
        uint32              timer_id;
        biqueue_node_t      node;
    };
};

typedef struct st_time_wheel
{
    void          *arg;

    /*时间轮的槽，其中每个元素指向一个定时器链表，链表无序*/
    tw_timer_t    *slots[TIME_WHEEL_N];
    uint32         cur_slot; /*时间轮的当前槽*/

    biqueue_t     *timer_pool;
} time_wheel_t;

time_wheel_t* time_wheel_create(uint32 timer_count);
void time_wheel_destroy(time_wheel_t *tw);

/*根据定时值timetout创建一个定时器，并插入它合适的槽中*/
tw_timer_t* time_wheel_set_timer(time_wheel_t *tw, uint32 count100ms, tw_timer_callback_func func, uint16 event, void *arg);
void time_wheel_reset_timer(time_wheel_t *tw, tw_timer_t *timer, uint32 count100ms);

/*删除目标定时器 timer */
void time_wheel_del_timer(time_wheel_t *tw, tw_timer_t *timer);

tw_timer_t* time_wheel_get_timer(time_wheel_t *tw, uint32 timer_id);

/*SI 时间到后，调用该函数，时间轮向前滚动一个槽的间隔*/
/* 使用方法，每隔一秒执行一次这函数 */
void time_wheel_tick(time_wheel_t *tw);


#ifdef __cplusplus
}
#endif

#endif
