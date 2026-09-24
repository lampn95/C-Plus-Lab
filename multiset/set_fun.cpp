#include "set_fun.h"
#include <iostream>
#include <ostream>
#include <set>

class IPerson {
public:
    virtual ~IPerson() = default;
    // Thêm const vào cuối hàm thuần ảo
    virtual void say_hello() const = 0;
};

class Person : public IPerson {
public:
    Person(int age) : age_(age) {}
    ~Person() override {}

    // Thêm const vào cuối hàm override
    void say_hello() const override {
        std::cout << "Person::say_hello() with age: " << age_ << std::endl;
    }

    bool operator < (const Person& o) const {
        return age_ < o.age_;
    }

private:
    int age_;
};

int main() {
    Person p(10);
    p.say_hello();

    std::multiset<Person> set_person;
    set_person.insert(p);
    set_person.insert({8});

    // Bây giờ gọi được bình thường vì x là const Person&
    for (const Person& x: set_person) {
        x.say_hello();
    }
    return 0;
}