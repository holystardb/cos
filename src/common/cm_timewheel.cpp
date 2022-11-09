#include "cm_timewheel.h"


static tw_timer_t* alloc_timer(time_wheel_t* tw)
{
    biqueue_node_t *node;
    tw_timer_t     *timer;

    node = biqueue_alloc(tw->timer_pool);
    if (node == NULL) {
        return NULL;
    }
    timer = OBJECT_OF_QUEUE_NODE(tw_timer_t, node, node);
    return timer;
}

static void free_timer(time_wheel_t* tw, tw_timer_t* timer)
{
    biqueue_node_t *node;
    node = QUEUE_NODE_OF_OBJECT(timer, node);
    biqueue_free(tw->timer_pool, node);
}

time_wheel_t* time_wheel_create(uint32 timer_count)
{
    uint32        i;
    time_wheel_t *tw;
    
    tw = (time_wheel_t *)malloc(sizeof(time_wheel_t));
    tw->cur_slot = 0;
    for(i = 0; i < TIME_WHEEL_N; i++) {
        tw->slots[i] = NULL;  /*初始化每个槽的头结点*/
    }

    tw->timer_pool = biqueue_init(sizeof(tw_timer_t), timer_count);
    if (tw->timer_pool == NULL) {
        free(tw);
        return NULL;
    }
    return tw;
}

void time_wheel_destroy(time_wheel_t *tw)
{
    uint32      i;
    //遍历整个槽，
    for(i = 0; i < TIME_WHEEL_N; i++) {
        tw_timer_t *tmp = tw->slots[i];
        while(tmp) {
            tw->slots[i] = tmp->next;
            free_timer(tw, tmp);
            tmp = tw->slots[i];
        }
    }
    free(tw);
}

/*根据定时值timetout创建一个定时器，并插入它合适的槽中*/
tw_timer_t* time_wheel_set_timer(time_wheel_t *tw, uint32 count100ms, tw_timer_callback_func func, uint16 event, void *arg)
{
    tw_timer_t* timer;
    uint32      ticks;
    
    if(count100ms < TIME_WHEEL_SI) {
        ticks = 1;
    } else {
        ticks = count100ms / TIME_WHEEL_SI;
    }

    /* 创建新的定时器, 他在时间轮转动rotation圈滞后被触发，且位于第ts个槽上 */
    timer = alloc_timer(tw);
    if (timer == NULL) {
        return NULL;
    }
    /*计算待插入的定时器z在时间轮转动多少圈后被触发*/
    timer->rotation = ticks / TIME_WHEEL_N;
    /*计算待插入的定时器应该被插入哪个槽中*/
    timer->time_slot = (tw->cur_slot + (ticks % TIME_WHEEL_N)) % TIME_WHEEL_N;
    timer->arg = arg;
    timer->event = event;
    timer->func = func;

    //如果timer->time_slot个槽中尚无定时器，则把新建的定时器插入其中，并将该定时器设置为该槽的头结点
    if(!tw->slots[timer->time_slot]) {
        //printf("add timer,rotation is %d,ts is %d,cur_slot is %d\n\n", timer->rotation,timer->time_slot, tw->cur_slot);
        tw->slots[timer->time_slot] = timer;
    } else { /*否则,将定时器插入timer->time_slot槽中*/
        timer->next = tw->slots[timer->time_slot];
        tw->slots[timer->time_slot]->prev = timer;
        tw->slots[timer->time_slot] = timer;
    }

    return timer;
}

void time_wheel_reset_timer(time_wheel_t *tw, tw_timer_t *timer, uint32 count100ms)
{
    uint32 ticks, rotation, ts;

    if(count100ms < TIME_WHEEL_SI) {
        ticks = 1;
    } else {
        ticks = count100ms / TIME_WHEEL_SI;
    }

    /*计算待插入的定时器z在时间轮转动多少圈后被触发*/
    rotation = ticks / TIME_WHEEL_N;

    /*计算待插入的定时器应该被插入哪个槽中*/
    ts = (tw->cur_slot + (ticks % TIME_WHEEL_N)) % TIME_WHEEL_N;

    /* 创建新的定时器, 他在时间轮转动rotation圈滞后被触发，且位于第ts个槽上 */
    timer->rotation = rotation;
    timer->time_slot = ts;

    //如果ts个槽中尚无定时器，则把新建的定时器插入其中，并将该定时器设置为该槽的头结点
    if(!tw->slots[ts]) {
        //printf("add timer ,rotation is %d,ts is %d,cur_slot is %d\n\n", rotation,ts,cur_slot);
        tw->slots[ts] = timer;
    } else { /*否则,将定时器插入ts槽中*/
        timer->next = tw->slots[ts];
        tw->slots[ts]->prev = timer;
        tw->slots[ts] = timer;
    }
}

tw_timer_t* time_wheel_get_timer(time_wheel_t *tw, uint32 timer_id)
{
    biqueue_node_t *node;
    tw_timer_t     *timer;

    node = biqueue_get_node(tw->timer_pool, timer_id);
    if (node == NULL) {
        return NULL;
    }
    timer = OBJECT_OF_QUEUE_NODE(tw_timer_t, node, node);
    return timer;
}

/*删除目标定时器 timer */
void time_wheel_del_timer(time_wheel_t *tw, tw_timer_t *timer)
{
    uint32 ts = timer->time_slot;
    /*slots[ts] 是目标定时器所在槽的头结点。如果目标定时器就是该头结点
    * 则需要重置第ts个槽头结点
    */
    if(timer == tw->slots[ts]) {
        tw->slots[ts] = tw->slots[ts]->next;
        if(tw->slots[ts]) {
            tw->slots[ts]->prev = NULL;
        }
    } else {
        timer->prev->next = timer->next;
        if(timer->next) {
            timer->next->prev = timer->prev;
        }
    }
    free_timer(tw, timer);
}


/*SI 时间到后，调用该函数，时间轮向前滚动一个槽的间隔*/
/* 使用方法，每隔一秒执行一次这函数 */
void time_wheel_tick(time_wheel_t *tw)
{
    tw_timer_t *tmp = tw->slots[tw->cur_slot];
    while(tmp) {
        /*如果定时器的rotation值大于0，则他在这一轮不起作用*/
        if(tmp->rotation > 0) {
            tmp->rotation--;
            tmp = tmp->next;
        } else { /*否则，说明定时器已经到期了，于是执行定时任务，然后删除定时器*/
            tmp->func(tmp, tmp->arg);
            if(tmp == tw->slots[tw->cur_slot]) {
                //printf("delete header in cur_slot\n");
                tw->slots[tw->cur_slot] = tmp->next;
                free_timer(tw, tmp);
                if(tw->slots[tw->cur_slot]) {
                    tw->slots[tw->cur_slot]->prev = NULL;
                }
                tmp = tw->slots[tw->cur_slot];
            } else {
                tmp->prev->next = tmp->next;
                if(tmp->next) {
                    tmp->next->prev = tmp->prev;
                }
                tw_timer_t *tmp2 = tmp->next;
                free_timer(tw, tmp);
                tmp = tmp2;
            }
        }
    }
    /*更新当前时间轮的槽，以反应时间轮的转动*/
    tw->cur_slot++;
    tw->cur_slot = tw->cur_slot % TIME_WHEEL_N;
}
