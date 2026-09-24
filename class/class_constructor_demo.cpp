#include<iostream>
#include<string>

using namespace std;

class Box {
    public:
        Box(): label_("default"), size_(0) {
            cout << "Default constructor called" << endl;
        }
            
        Box(string&& label, int size): label_(std::move(label)), size_(size) {
            cout << "Parameterized constructor called" << endl;
        }
        Box(const Box& other): label_(other.label_), size_(other.size_ + 1) {
            cout << "Copy constructor called" << endl;
        }

        void display() {
            cout << "Box label: " << label_ << ", size: " << size_ << endl;
        }

          // Destructor — gọi tự động khi object hết scope
        ~Box() {
            std::cout << "[dtor]         " << label_ << " with size " << size_ << '\n';
        }

    private:
        string label_;
        int size_;
};

int main() {
    Box box1; // Default constructor
    Box box2("Large Box", 10); // Parameterized constructor

    
    Box box3 = box2; // Copy constructor

    return 0;

}