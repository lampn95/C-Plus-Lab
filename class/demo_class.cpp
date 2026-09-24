#include<iostream>

using namespace std;

class Person {
    public:
        Person(std::string&& name, int age, std::vector<int>&& pointArr): name_(std::move(name)), age_(age), pointArr(std::move(pointArr)) {

        }

        void introduce() {
            std::cout << "Hello, my name is " << name_ << " and I am " << age_ << " years old." << std::endl;
        }
        void printPoints() {
            std::cout << "My points are: ";
            for (const auto& point : pointArr) {
                std::cout << point << " ";
            }
            std::cout << std::endl;
        }
    private:
        std::string name_;
        int age_;
        std::vector<int> pointArr;
};

int main() {
    std::vector<int> points = {1, 2, 3, 4, 5};
    Person ps = Person("Alice", 30, std::move(points));
    ps.introduce();
    ps.printPoints();
    std::cout << points.size() << std::endl; // points vector is now empty after move
    return 0;
}