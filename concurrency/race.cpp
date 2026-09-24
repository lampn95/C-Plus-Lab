#include<iostream>
#include<thread>
#include<vector>
#include<mutex>

constexpr int NUM_THREADS = 5;
constexpr int NUM_ICRE = 1000; 

int counter_bad = 0;

void inc_bad() {
    for (int i = 0; i < NUM_ICRE; ++i) {
        ++counter_bad; // Không có bảo vệ,
    }
}

int counter_good = 0;
std::mutex mtx;

void inc_good() {
    for (int i = 0; i < NUM_ICRE; ++i) {
        std::lock_guard<std::mutex> lock(mtx); // Bảo vệ bằng mutex
        ++counter_good;
        mtx.unlock();
    }
}

template<typename Func>
void run(Func fn) {
    std::vector<std::thread> threads;
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back(fn);
    }
    for (auto& t : threads) {
        t.join();
    }
}

int main() {
    run(inc_bad);
    std::cout << "Counter (bad): " << counter_bad << " (Expected: " << NUM_THREADS * NUM_ICRE << ")" << std::endl;

    run(inc_good);
    std::cout << "Counter (good): " << counter_good << " (Expected: " << NUM_THREADS * NUM_ICRE << ")" << std::endl;

    return 0;
}