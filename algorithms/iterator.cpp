#include <iostream>
#include <iterator>
#include <string>
#include <algorithm>
#include <vector>
#include <ranges>
using namespace std;

bool has_c(const string& s, char c)
{
    return find(s.begin(), s.end(), c) != s.end();
}

template <typename C, typename V>
auto find_all(C& c, V v) {
    vector<ranges::range_value_t<C>*> res;
    for(auto& x: c) {
        if (x == v) {
            res.push_back(&x);
        }
    }
    return res;
}

ostream_iterator<string> oo{cout};


int main()
{
    string s = "hello";
    char c = 'o';
    cout << has_c(s, c) << endl;

    // *oo = "Hello, ";
    // ++oo;
    // *oo = "world!";
    vector<string> vv = {"Hello", " world!"};
    ranges::copy(vv, oo);
    cout << endl;
    return 0;
}