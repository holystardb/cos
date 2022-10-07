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
        tw->slots[i] = NULL;  /*��ʼ��ÿ���۵�ͷ���*/
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
    //���������ۣ�
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

/*���ݶ�ʱֵtimetout����һ����ʱ���������������ʵĲ���*/
tw_timer_t* time_wheel_set_timer(time_wheel_t *tw, uint32 count100ms, tw_timer_callback_func func, uint16 event, void *arg)
{
    tw_timer_t* timer;
    uint32      ticks;
    
    if(count100ms < TIME_WHEEL_SI) {
        ticks = 1;
    } else {
        ticks = count100ms / TIME_WHEEL_SI;
    }

    /* �����µĶ�ʱ��, ����ʱ����ת��rotationȦ�ͺ󱻴�������λ�ڵ�ts������ */
    timer = alloc_timer(tw);
    if (timer == NULL) {
        return NULL;
    }
    /*���������Ķ�ʱ��z��ʱ����ת������Ȧ�󱻴���*/
    timer->rotation = ticks / TIME_WHEEL_N;
    /*���������Ķ�ʱ��Ӧ�ñ������ĸ�����*/
    timer->time_slot = (tw->cur_slot + (ticks % TIME_WHEEL_N)) % TIME_WHEEL_N;
    timer->arg = arg;
    timer->event = event;
    timer->func = func;

    //���timer->time_slot���������޶�ʱ��������½��Ķ�ʱ���������У������ö�ʱ������Ϊ�ò۵�ͷ���
    if(!tw->slots[timer->time_slot]) {
        //printf("add timer,rotation is %d,ts is %d,cur_slot is %d\n\n", timer->rotation,timer->time_slot, tw->cur_slot);
        tw->slots[timer->time_slot] = timer;
    } else { /*����,����ʱ������timer->time_slot����*/
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

    /*���������Ķ�ʱ��z��ʱ����ת������Ȧ�󱻴���*/
    rotation = ticks / TIME_WHEEL_N;

    /*���������Ķ�ʱ��Ӧ�ñ������ĸ�����*/
    ts = (tw->cur_slot + (ticks % TIME_WHEEL_N)) % TIME_WHEEL_N;

    /* �����µĶ�ʱ��, ����ʱ����ת��rotationȦ�ͺ󱻴�������λ�ڵ�ts������ */
    timer->rotation = rotation;
    timer->time_slot = ts;

    //���ts���������޶�ʱ��������½��Ķ�ʱ���������У������ö�ʱ������Ϊ�ò۵�ͷ���
    if(!tw->slots[ts]) {
        //printf("add timer ,rotation is %d,ts is %d,cur_slot is %d\n\n", rotation,ts,cur_slot);
        tw->slots[ts] = timer;
    } else { /*����,����ʱ������ts����*/
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

/*ɾ��Ŀ�궨ʱ�� timer */
void time_wheel_del_timer(time_wheel_t *tw, tw_timer_t *timer)
{
    uint32 ts = timer->time_slot;
    /*slots[ts] ��Ŀ�궨ʱ�����ڲ۵�ͷ��㡣���Ŀ�궨ʱ�����Ǹ�ͷ���
    * ����Ҫ���õ�ts����ͷ���
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


/*SI ʱ�䵽�󣬵��øú�����ʱ������ǰ����һ���۵ļ��*/
/* ʹ�÷�����ÿ��һ��ִ��һ���⺯�� */
void time_wheel_tick(time_wheel_t *tw)
{
    tw_timer_t *tmp = tw->slots[tw->cur_slot];
    while(tmp) {
        /*�����ʱ����rotationֵ����0����������һ�ֲ�������*/
        if(tmp->rotation > 0) {
            tmp->rotation--;
            tmp = tmp->next;
        } else { /*����˵����ʱ���Ѿ������ˣ�����ִ�ж�ʱ����Ȼ��ɾ����ʱ��*/
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
    /*���µ�ǰʱ���ֵĲۣ��Է�Ӧʱ���ֵ�ת��*/
    tw->cur_slot++;
    tw->cur_slot = tw->cur_slot % TIME_WHEEL_N;
}
