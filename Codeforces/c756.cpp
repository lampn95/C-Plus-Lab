#include <iostream>
#include <vector>
#include <algorithm>

using namespace std;

const int MAXM = 100005;

// Nút của Segment tree
struct Node {
    int sum;
    int max_suf;
} tree[4 * MAXM];

// Mảng lưu giá trị x được push vào tại thời điểm p
int val_at[MAXM];

// Cập nhật vị trí idx trên Segment Tree với giá trị val (+1 hoặc -1)
void update(int node, int L, int R, int idx, int val) {
    if (L == R) {
        tree[node].sum = val;
        tree[node].max_suf = val;
        return;
    }
    
    int mid = (L + R) / 2;
    if (idx <= mid) {
        update(2 * node, L, mid, idx, val);
    } else {
        update(2 * node + 1, mid + 1, R, idx, val);
    }
    
    // Tổng của đoạn bằng tổng 2 con
    tree[node].sum = tree[2 * node].sum + tree[2 * node + 1].sum;
    
    // Hậu tố lớn nhất có thể là hậu tố lớn nhất của con phải, 
    // hoặc bao gồm toàn bộ con phải cộng với hậu tố lớn nhất của con trái
    tree[node].max_suf = max(tree[2 * node + 1].max_suf, 
                             tree[2 * node].max_suf + tree[2 * node + 1].sum);
}

// Truy vấn tìm vị trí ngoài cùng bên phải có suffix_sum > 0
int query(int node, int L, int R, int right_sum) {
    // Nếu đi tới lá thì đây chính là phần tử nằm ở đỉnh stack
    if (L == R) {
        return val_at[L];
    }
    
    int mid = (L + R) / 2;
    int right_node = 2 * node + 1;
    int left_node = 2 * node;
    
    // Nếu bên cây con phải tồn tại vị trí đáp ứng được suffix sum > 0
    if (tree[right_node].max_suf + right_sum > 0) {
        return query(right_node, mid + 1, R, right_sum);
    } else {
        // Ngược lại, tìm ở cây con trái, nhớ cộng dồn tổng của cây con phải vào right_sum
        return query(left_node, L, mid, right_sum + tree[right_node].sum);
    }
}

int main() {
    // Tối ưu I/O để chạy nhanh hơn
    ios_base::sync_with_stdio(false);
    cin.tie(NULL);

    int m;
    if (!(cin >> m)) return 0;

    for (int i = 0; i < m; ++i) {
        int p, t;
        cin >> p >> t;
        
        if (t == 1) { // Lệnh push
            int x;
            cin >> x;
            val_at[p] = x;
            update(1, 1, m, p, 1);
        } else {      // Lệnh pop
            update(1, 1, m, p, -1);
        }

        // Nếu tổng hậu tố lớn nhất của toàn bộ cây > 0, tức là stack không rỗng
        if (tree[1].max_suf > 0) {
            cout << query(1, 1, m, 0) << "\n";
        } else {
            cout << "-1\n";
        }
    }

    return 0;
}