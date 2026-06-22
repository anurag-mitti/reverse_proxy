// ThreadPool.hpp
//
// A fixed-size thread pool that pulls tasks from a shared queue.
//
// Key properties:
//  - Queue access is mutex-protected with a tiny critical section
//    (push_back/pop_front only) -- no data races on pop.
//  - Worker wakeup uses std::counting_semaphore instead of
//    condition_variable, so there are no spurious wakeups and no
//    thundering herd: each enqueue() releases exactly one permit,
//    so exactly one waiting worker can acquire it.
//  - If a task throws, the exception is caught inside the worker
//    (so it can't std::terminate() your whole program) and the task
//    is pushed back onto the front of the queue for retry.
//  - Optional per-task timeout: if a task overruns, the worker
//    abandons it (best-effort -- C++ cannot safely kill a running
//    thread) and a replacement worker is spawned to keep pool size
//    constant.


#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <semaphore>
#include <thread>
#include <vector>

class ThreadPool {
public:
    using Task = std::function<void()>;

   //num threads is the poll size, timeout is the thread time out
    explicit ThreadPool(size_t num_threads = 8,
                         std::chrono::milliseconds task_timeout = std::chrono::milliseconds(0))
        : sem_(0), stop_(false), task_timeout_(task_timeout)
    {
        for (size_t i = 0; i < num_threads; ++i) {
            spawn_worker();
        }
    }

    ~ThreadPool() {
        shutdown();
    }

    ThreadPool(const ThreadPool&)            = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

  //mian func to add the task
    void enqueue(Task task) {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            queue_.push_back(std::move(task));
        }
        sem_.release(1); // wake exactly one idle worker
    }

   
    void shutdown() {
        if (stop_.exchange(true)) return; 

        size_t n;
        {
            std::lock_guard<std::mutex> lock(workers_mutex_);
            n = workers_.size();
        }
        sem_.release(static_cast<ptrdiff_t>(n)); 

        std::lock_guard<std::mutex> lock(workers_mutex_);
        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
    }

    size_t pending() const {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return queue_.size();
    }

private:
    void spawn_worker() {
        std::lock_guard<std::mutex> lock(workers_mutex_);
        workers_.emplace_back([this] { worker_loop(); });
    }

    bool try_pop(Task& out) {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (queue_.empty()) return false;
        out = std::move(queue_.front());
        queue_.pop_front();
        return true;
    }

    void requeue_front(Task task) {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queue_.push_front(std::move(task));
    }

    void worker_loop() {
        while (true) {
           
            sem_.acquire();

            if (stop_.load(std::memory_order_acquire)) {
                return; 
            }

            Task task;
            if (!try_pop(task)) {
             
                continue;
            }

            run_task(std::move(task));
        }
    }

    void run_task(Task task) {
        if (task_timeout_.count() == 0) {
            execute_catching_exceptions(std::move(task));
            return;
        }


        auto promise = std::make_shared<std::promise<void>>();
        std::future<void> fut = promise->get_future();

        std::thread helper([promise, task]() mutable {
            try {
                task();
                promise->set_value();
            } catch (...) {
                try { promise->set_exception(std::current_exception()); }
                catch (...) {  }
            }
        });
        helper.detach(); 

        if (fut.wait_for(task_timeout_) == std::future_status::timeout) {
            std::cerr << "[ThreadPool] task exceeded "
                      << task_timeout_.count()
                      << "ms timeout; abandoning it and replacing this worker.\n";
            if (!stop_.load(std::memory_order_acquire)) {
                spawn_worker(); // keep pool size constant
            }
            return; // this worker thread's loop call returns; loop ends here
        }

        try {
            fut.get(); // rethrows if the task threw
        } catch (...) {
            handle_failed_task(task);
        }
    }

    void execute_catching_exceptions(Task task) {
        try {
            task();
        } catch (...) {
            handle_failed_task(task);
        }
    }

    void handle_failed_task(Task& task) {
        std::cerr << "[ThreadPool] task threw an exception; re-queuing it.\n";
        requeue_front(task);
        sem_.release(1); // make sure someone picks it back up
    }

    std::vector<std::thread> workers_;
    std::mutex                workers_mutex_;

    std::deque<Task>          queue_;
    mutable std::mutex        queue_mutex_;

    std::counting_semaphore<> sem_;
    std::atomic<bool>         stop_;
    std::chrono::milliseconds task_timeout_;
};