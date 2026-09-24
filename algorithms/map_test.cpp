#include <iostream>
#include <string>
#include <string_view>

#include "absl/container/flat_hash_map.h"

// absl::flat_hash_map is Abseil's Swiss table: open addressing, keys stored
// inline. Faster and denser than node-based std::unordered_map in most cases.
int main() {
    absl::flat_hash_map<std::string, int> scores = {
        {"alice", 90},
        {"bob", 75},
    };

    scores["carol"] = 88;
    scores.insert({"dave", 70});
    scores.try_emplace("alice", 100);  // alice exists -> no overwrite

    std::cout << "alice=" << scores.at("alice") << "\n";
    std::cout << "contains bob? " << std::boolalpha
              << scores.contains("bob") << "\n";

    if (auto it = scores.find("carol"); it != scores.end()) {
        std::cout << it->first << "=" << it->second << "\n";
    }

    scores.erase("dave");

    std::cout << "size=" << scores.size()
              << " capacity=" << scores.capacity() << "\n";

    for (const auto& [name, score] : scores) {
        std::cout << name << " -> " << score << "\n";
    }

    // string_view lookup avoids allocating a temporary std::string.
    std::string_view key = "bob";
    std::cout << "lookup via string_view: "
              << scores.contains(key) << "\n";
    return 0;
}
