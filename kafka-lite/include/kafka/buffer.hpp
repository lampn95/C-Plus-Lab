#pragma once

#include "concepts.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

namespace kafka {

using ByteSpan = std::span<const std::byte>;
using MutableByteSpan = std::span<std::byte>;

template <ByteRange R>
[[nodiscard]] inline ByteSpan as_bytes(const R& r) noexcept {
    return ByteSpan{reinterpret_cast<const std::byte*>(std::data(r)), std::size(r)};
}

[[nodiscard]] inline ByteSpan as_bytes(std::string_view s) noexcept {
    return ByteSpan{reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

[[nodiscard]] inline std::string_view as_string_view(ByteSpan b) noexcept {
    return std::string_view{reinterpret_cast<const char*>(b.data()), b.size()};
}

[[nodiscard]] constexpr std::size_t byte_size(ByteRange auto const& r) noexcept {
    return std::size(r);
}

template <ByteRange... Parts>
    requires (sizeof...(Parts) > 0)
[[nodiscard]] constexpr std::size_t total_size(const Parts&... parts) noexcept {
    return (byte_size(parts) + ...);
}

template <ByteRange... Parts>
    requires (sizeof...(Parts) == 0)
[[nodiscard]] constexpr std::size_t total_size(const Parts&...) noexcept {
    return 0;
}

inline void store_le32(std::byte* p, std::uint32_t v) noexcept {
    p[0] = std::byte(v);
    p[1] = std::byte(v >> 8);
    p[2] = std::byte(v >> 16);
    p[3] = std::byte(v >> 24);
}

inline void store_le64(std::byte* p, std::uint64_t v) noexcept {
    store_le32(p, static_cast<std::uint32_t>(v));
    store_le32(p + 4, static_cast<std::uint32_t>(v >> 32));
}

[[nodiscard]] inline std::uint32_t load_le32(const std::byte* p) noexcept {
    return static_cast<std::uint32_t>(p[0]) |
           (static_cast<std::uint32_t>(p[1]) << 8) |
           (static_cast<std::uint32_t>(p[2]) << 16) |
           (static_cast<std::uint32_t>(p[3]) << 24);
}

[[nodiscard]] inline std::uint64_t load_le64(const std::byte* p) noexcept {
    return static_cast<std::uint64_t>(load_le32(p)) |
           (static_cast<std::uint64_t>(load_le32(p + 4)) << 32);
}

// Gather list: keep payload as views, memcpy once into the mmap log (no extra buffer).
class GatherList {
public:
    static constexpr std::size_t kMaxParts = 16;

    template <ByteRange... Parts>
    void add(const Parts&... parts) {
        (push(as_bytes(parts)), ...);
    }

    void push(ByteSpan part) {
        if (part.empty()) {
            return;
        }
        if (n_ >= kMaxParts) {
            overflow_ = true;
            return;
        }
        parts_[n_++] = part;
    }

    [[nodiscard]] bool overflow() const noexcept { return overflow_; }
    [[nodiscard]] std::size_t part_count() const noexcept { return n_; }

    [[nodiscard]] std::size_t size() const noexcept {
        std::size_t n = 0;
        for (std::size_t i = 0; i < n_; ++i) {
            n += parts_[i].size();
        }
        return n;
    }

    // Single sequential write into dest — this is the only payload copy (into page cache).
    std::size_t copy_to(MutableByteSpan dest) const noexcept {
        std::size_t written = 0;
        for (std::size_t i = 0; i < n_; ++i) {
            const auto part = parts_[i];
            if (written + part.size() > dest.size()) {
                break;
            }
            std::memcpy(dest.data() + written, part.data(), part.size());
            written += part.size();
        }
        return written;
    }

    template <ByteRange... Parts>
    static std::size_t gather_copy(MutableByteSpan dest, const Parts&... parts) noexcept {
        GatherList g;
        g.add(parts...);
        return g.copy_to(dest);
    }

private:
    std::array<ByteSpan, kMaxParts> parts_{};
    std::size_t n_ = 0;
    bool overflow_ = false;
};

[[nodiscard]] inline bool pointer_in_span(const void* p, ByteSpan span) noexcept {
    if (p == nullptr || span.empty()) {
        return false;
    }
    auto* b = static_cast<const std::byte*>(p);
    return b >= span.data() && b < span.data() + span.size();
}

}  // namespace kafka
