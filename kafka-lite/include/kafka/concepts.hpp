#pragma once

#include <concepts>
#include <cstddef>
#include <ranges>
#include <string_view>
#include <type_traits>

namespace kafka {

// Combine multiple callables into one visitor (fold of operator()).
template <class... Ts>
struct Overloaded : Ts... {
    using Ts::operator()...;
};

template <class... Ts>
Overloaded(Ts...) -> Overloaded<Ts...>;

template <typename T>
concept ByteRange =
    std::ranges::contiguous_range<T> &&
    (sizeof(std::ranges::range_value_t<T>) == 1) &&
    !std::is_bounded_array_v<std::remove_cvref_t<T>>;

template <typename T>
concept RecordLike = requires(const T& r) {
    { r.key() } -> std::convertible_to<std::string_view>;
    { r.value() } -> std::convertible_to<std::string_view>;
};

template <typename F, typename Record>
concept RecordHandler = requires(F&& f, const Record& r) {
    std::forward<F>(f)(r);
};

template <typename T>
concept TopicName = std::convertible_to<T, std::string_view>;

template <typename... Ts>
concept AllByteRanges = (ByteRange<std::remove_cvref_t<Ts>> && ...);

template <typename... Ts>
concept AllRecordLike = (RecordLike<std::remove_cvref_t<Ts>> && ...);

template <typename... Ts>
concept AllTopicNames = (TopicName<std::remove_cvref_t<Ts>> && ...);

}  // namespace kafka
