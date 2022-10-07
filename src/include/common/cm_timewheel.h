#ifndef _CM_TIME_WHEEL_H
#define _CM_TIME_WHEEL_H

#include "cm_type.h"
#include "cm_biqueue.h"


#define BUFFER_SIZE 64

#ifdef __cplusplus
extern "C" {
#endif

#define INVALID_TIMER_ID        0xFFFFFFFF

/* ʱ�����ϲ۵����� */
#define TIME_WHEEL_N            600

/* ÿ100msʱ����ת��һ�Σ����� ���Ϊ100ms */
#define TIME_WHEEL_SI           1

typedef struct st_tw_timer   tw_timer_t;
typedef void* (*tw_timer_callback_func)(tw_timer_t *timer, void *arg);

/*��ʱ����*/
struct st_tw_timer
{
    void                   *arg;  /*�ͻ�����*/
    tw_timer_callback_func  func;
    uint16                  event;

    uint32                  rotation;  /*��¼��ʱ����ʱ����ת����Ȧ����Ч*/
    uint32                  time_slot;  /*��¼��ʱ������ʱ�����ϵ��ĸ���(��Ӧ��������ͬ)*/
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

    /*ʱ���ֵĲۣ�����ÿ��Ԫ��ָ��һ����ʱ��������������*/
    tw_timer_t    *slots[TIME_WHEEL_N];
    uint32         cur_slot; /*ʱ���ֵĵ�ǰ��*/

    biqueue_t     *timer_pool;
} time_wheel_t;

time_wheel_t* time_wheel_create(uint32 timer_count);
void time_wheel_destroy(time_wheel_t *tw);

/*���ݶ�ʱֵtimetout����һ����ʱ���������������ʵĲ���*/
tw_timer_t* time_wheel_set_timer(time_wheel_t *tw, uint32 count100ms, tw_timer_callback_func func, uint16 event, void *arg);
void time_wheel_reset_timer(time_wheel_t *tw, tw_timer_t *timer, uint32 count100ms);

/*ɾ��Ŀ�궨ʱ�� timer */
void time_wheel_del_timer(time_wheel_t *tw, tw_timer_t *timer);

tw_timer_t* time_wheel_get_timer(time_wheel_t *tw, uint32 timer_id);

/*SI ʱ�䵽�󣬵��øú�����ʱ������ǰ����һ���۵ļ��*/
/* ʹ�÷�����ÿ��һ��ִ��һ���⺯�� */
void time_wheel_tick(time_wheel_t *tw);


#ifdef __cplusplus
}
#endif

#endif
