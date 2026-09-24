#include<iostream>
#include<type_strait>
#include<vector>
//using namespace std;

template<typename T>
class Stack {
public:
    void push(T const& ele);
    void pop();
    T const& top() const;
    bool empty() const;
private:
    std::vector<T> elems;
};

template<typename T>
void Stack<T>::push(T const& ele) {
    elems.push_back(ele);
}

template<typename T>
void Stack<T>::pop() {
    assert(!elems.empty());
    elems.pop_back();
}

template<typename T>
T const& Stack<T>::top() const {
    assert(!elems.empty());
    return elems.back();
}

template<typename T>
class C {
    static_assert(std::is_default_constructible_v<T>, "Class C requires default constructible elements");
};