#include "tasksys.h"
#include <thread>
#include <mutex>

//debug 
#include <cstdio>
#include <iostream>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    max_threads_ = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}


//Function to be run by individual threads 
//Continuously runs tasks until all tasks are complete
void TaskSystemParallelSpawn::doWork(IRunnable* runnable) {
    while (current_task_num_ < total_num_tasks_) {
        int nextTask = current_task_num_++;
        runnable -> runTask(nextTask, total_num_tasks_);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //Initialize values
    max_threads_ = num_total_tasks; 
    total_num_tasks_ = num_total_tasks; 
    current_task_num_ = 0;

    //Here, we make all of our threads
    std::vector<std::thread> threads(max_threads_);


    //Launch every thread. Each thread runs tasks until all tasks are done. 
    for (int i = 0; i < max_threads_; ++i) {
        threads[i] = std::thread(&TaskSystemParallelSpawn::doWork, this, runnable);
    }
    
    //Do not return until all threads have joined (ie also returned)
    for (auto& thread : threads) {
        thread.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}


TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    current_runnable_ = nullptr;
    are_tasks_running_ = false;
    is_function_returning_ = false;


    for (int i = 0; i < num_threads; ++i) {
        threads_.push_back(std::thread(&TaskSystemParallelThreadPoolSpinning::doWorkOrSpin, this));
    }
 
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    state_lock_.lock();
    is_function_returning_ = true;
    state_lock_.unlock();
    
    for (auto& thread : threads_) {

        if (thread.joinable()) {
            thread.join();
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::doWorkOrSpin() {
    while (true) {
        IRunnable* thread_runnable = nullptr;
        int next_task = -1;
        {
            std::lock_guard<std::mutex> lock(state_lock_);

            if (is_function_returning_) return;

            if (num_tasks_remaining_ > 0 && current_runnable_ != nullptr) {
                next_task = --num_tasks_remaining_;
                thread_runnable = current_runnable_;
            }
        }
        if (thread_runnable && next_task >= 0) {
            thread_runnable->runTask(next_task, total_tasks);

            state_lock_.lock();
            num_tasks_completed_++;

            if (num_tasks_completed_ == total_tasks) {
                are_tasks_running_ = false;
                current_runnable_ = nullptr;
            }
            state_lock_.unlock();
        } 
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {

    
    state_lock_.lock();
    current_runnable_ = runnable;
    total_tasks = num_total_tasks;
    num_tasks_remaining_ = num_total_tasks;
    num_tasks_completed_ = 0;
    are_tasks_running_ = true;
    state_lock_.unlock();

    while (true) {
        {
            std::lock_guard<std::mutex> lock(state_lock_);
            if (!are_tasks_running_) break;
        }
    }
    
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    current_runnable_ = nullptr;
    is_function_returning_ = false;
    are_tasks_running_ = false;


    num_threads_ = num_threads;
    for (int i = 0; i < num_threads_; i++) {
        threads_.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::doWorkOrSleep, this));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    task_lock_.lock();
    is_function_returning_ = true;
    task_lock_.unlock();
    waitUntilWork.notify_all();

    for (auto& thread: threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::doWorkOrSleep() {
    while (true) {
        int nextTask = -1;
        IRunnable* thread_runnable = nullptr; 
        {
            std::unique_lock<std::mutex> lock(task_lock_);


            waitUntilWork.wait(lock, [this] {
                return (current_runnable_ != nullptr && num_tasks_remaining_ > 0) || is_function_returning_;
            });


            if (is_function_returning_) return;

            if (num_tasks_remaining_ > 0 && current_runnable_ != nullptr) {
                nextTask = --num_tasks_remaining_;
                thread_runnable = current_runnable_;
            }
        }
        if (thread_runnable != nullptr && nextTask >= 0) {
            thread_runnable->runTask(nextTask, total_tasks_);

            task_lock_.lock(); 
            num_tasks_completed_++;
            if (num_tasks_completed_ == total_tasks_) {
                are_tasks_running_ = false;
                current_runnable_ = nullptr;
            }
            task_lock_.unlock();
        }

    }

}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    task_lock_.lock();
    current_runnable_ = runnable;
    total_tasks_ = num_total_tasks;
    num_tasks_remaining_ = num_total_tasks;
    num_tasks_completed_ = 0;
    are_tasks_running_ = true;
    waitUntilWork.notify_all();
    task_lock_.unlock();


    while (true) {
        std::lock_guard<std::mutex> lock(task_lock_);
        if (!are_tasks_running_) break;
    }

}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
