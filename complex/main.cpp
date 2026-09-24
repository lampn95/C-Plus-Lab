#include <iostream>
#include <type_traits>
#include <complex>

 // Bắt buộc để dùng ký tự 'i' cho số phức
using namespace std::complex_literals;

template<typename T>
concept Numeric = std::is_arithmetic_v<T>;

template<Numeric... T>
int sum(T... v) {
    return (v + ... + 1);
}

int main() {
    auto z1 = 2.0+3i;
}