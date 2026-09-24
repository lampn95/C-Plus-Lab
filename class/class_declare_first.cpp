//
// Created by Pham Lam on 9/5/26.
//

#include "class_declare_first.h"
#include <iostream>

class IPerson {
public:
    virtual ~IPerson() = default;
    virtual void say_hello() const = 0;
};

class Person : public IPerson {
public:
    // 1. Chỉ khai báo signature (prototype) bên trong class
    Person(int age);
    ~Person() override;

    void say_hello() const override;
    bool operator < (const Person& o) const;

private:
    int age_;
};

// ==========================================
// 2. Implement method ở bên ngoài class
// ==========================================

// Constructor implementation
Person::Person(int age) : age_(age) {}

// Destructor implementation
Person::~Person() {}

// Method implementation (cần có Person:: ở trước tên hàm)
void Person::say_hello() const {
    std::cout << "Person::say_hello() with age: " << age_ << std::endl;
}

// Operator implementation
bool Person::operator < (const Person& o) const {
    return age_ < o.age_;
}