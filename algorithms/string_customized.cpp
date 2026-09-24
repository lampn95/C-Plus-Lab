#include <algorithm>
#include <concepts>
#include <cstddef>
#include <iostream>

using size_t = std::size_t;

template<typename T>
concept CharType =
    std::same_as<T, char> ||
    std::same_as<T, wchar_t> ||
    std::same_as<T, char8_t> ||
    std::same_as<T, char16_t> ||
    std::same_as<T, char32_t>;

template<CharType T>
class BasicString {
    private:
    T* data_;
    size_t size_;
    size_t capacity_;

    static size_t cstring_size(const T* data) {
        size_t n = 0;
        if (data == nullptr) {
            return 0;
        }
        while (data[n] != T()) {
            ++n;
        }
        return n;
    }

    public:
    BasicString() : data_(new T[1]{}), size_(0), capacity_(0) {}
    BasicString(const T* data) : BasicString(data, cstring_size(data)) {}
    BasicString(const T* data, size_t size) : data_(new T[size + 1]), size_(size), capacity_(size) {
        std::copy(data, data + size, data_);
        data_[size] = T();
    }
    BasicString(const BasicString& other) : BasicString(other.data_, other.size_) {}
    BasicString& operator=(const BasicString& other) {
        if (this != &other) {
            BasicString tmp(other);
            std::swap(data_, tmp.data_);
            std::swap(size_, tmp.size_);
            std::swap(capacity_, tmp.capacity_);
        }
        return *this;
    }
    ~BasicString() {
        delete[] data_;
    }

    const T* data() const { return data_; }

    BasicString operator +(const BasicString& other) const {
        BasicString result;
        delete[] result.data_;
        result.size_ = size_ + other.size_;
        result.capacity_ = result.size_;
        result.data_ = new T[result.size_ + 1];
        std::copy(data_, data_ + size_, result.data_);
        std::copy(other.data_, other.data_ + other.size_, result.data_ + size_);
        result.data_[result.size_] = T();
        return result;
    }
};

int main() {
    BasicString<char> str1("Hello, ");
    BasicString<char> str2("World!");
    BasicString<char> str3 = str1 + str2;
    std::cout << str3.data() << std::endl;
    return 0;
}
