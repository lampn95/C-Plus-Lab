#include<iostream>
#include<string>

using namespace std;

class Animal {
    public:
        Animal(string&& name): name_(std::move(name)) {
            cout << "Animal constructor called" << endl;
        }
        void eat() {
            cout << "Animal is eating." << endl;
        }
    private:
        string name_;
};

class Dog : public Animal {
    public:
        Dog(string&& name, string&& breed): Animal(std::move(name)), breed_(std::move(breed)) {
            cout << "Dog constructor called" << endl;
        }
        void bark() {
            cout << "Dog is barking." << endl;
        }
    private:
        string breed_;
};

int main() {
    Dog myDog("Buddy", "Golden Retriever");
    myDog.eat(); // Inherited method from Animal
    myDog.bark(); // Dog's own method

    return 0;
}