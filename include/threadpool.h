/**
 * @Author      : Liu Zhiwen
 * @Create Date : 2021-01-06 20:19:46
 * @Contact     : 993096281@qq.com
 * @Description : 
 */
#ifndef _HBKV_THREAD_POOL_H_
#define _HBKV_THREAD_POOL_H_

#include <deque>
#include <vector>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

#include "lock.h"

using namespace std;

namespace hbkv {

struct TaskItem{
    void (*function)(void*);
    void* arg; 

    TaskItem(void (*function1)(void*), void* arg1) : function(function1), arg(arg1) {}
    ~TaskItem() {}
};

class ThreadPool {
public:
    ThreadPool(uint32_t count) : thread_count_(count), cv_(&mu_) {
        no_doing_thread_count_ = 0;
        shutdown_ = false;
        threads_.reserve(count);
        threads_.insert(threads_.begin(), count, 0);
        for(uint32_t i = 0; i < thread_count_; i++) {
            if(pthread_create(&(threads_[i]), NULL, &ThreadPool::BGThreadWrapper, this) != 0) {
                exit(-1);
            }
        }
    }
    ~ThreadPool(){
        mu_.Lock();
        shutdown_ = true;
        cv_.SignalAll();
        mu_.Unlock();
        for(auto it : threads_){
            pthread_join(it, NULL);
        }
    }

    static void* BGThreadWrapper(void* arg) {   //运行后台线程的容器
        reinterpret_cast<ThreadPool*>(arg)->BGThread();
        return NULL;
    }
    void BGThread(){  //后台线程循环执行任务
        while (true){
            mu_.Lock();
            while (deque_.empty() && (!shutdown_)){
                no_doing_thread_count_++;
                cv_.Wait();
                no_doing_thread_count_--;
            }
            if(shutdown_){
                mu_.Unlock();
                break;
            }
            void (*function)(void*) = deque_.front().function;
            void* arg = deque_.front().arg;
            deque_.pop_front();
            mu_.Unlock();
            (*function)(arg);
        }
        
    }

    void Schedule(void (*function)(void*), void* arg){  //添加任务，进行调度
        mu_.Lock();
        if(deque_.empty()){
            cv_.Signal();
        } else {
            cv_.SignalAll();
        }
        deque_.push_back(TaskItem(function, arg));
        mu_.Unlock();
    }

    static void* StartThreadWrapper(void* arg) {   
        TaskItem* task = reinterpret_cast<TaskItem*>(arg);
        task->function(task->arg);
        delete task;
        return NULL;
    }

    static pthread_t StartThread(void (*function)(void*), void* arg) {   //直接新建线程运行任务，
        pthread_t t;
        TaskItem* task = new TaskItem(function, arg);
        pthread_create(&t, NULL,  &StartThreadWrapper, task);
        return t;
    }

    void WaitForBGJob(){  //等待后台任务完成
        while(true){
            mu_.Lock();
            if(deque_.empty() && no_doing_thread_count_ == thread_count_){
                mu_.Unlock();
                break;
            }
            mu_.Unlock();
            sleep(2);
        }
    }


private:
    Mutex mu_;
    CondVar cv_;
    deque<TaskItem> deque_;
    vector<pthread_t> threads_;
    uint32_t thread_count_;
    bool shutdown_;
    uint32_t no_doing_thread_count_;
};

extern ThreadPool *thread_pool;

extern int InitThreadPool(uint32_t count);


} // namespace name








#endif