#pragma once

#include "concepts.hpp"

#include <functional>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

namespace kafka {

enum class Errc {
    unknown_topic,
    unknown_partition,
    offset_out_of_range,
    log_full,
    io_error,
    invalid_record,
    invalid_argument,
};

[[nodiscard]] constexpr std::string_view to_string(Errc e) noexcept {
    switch (e) {
        case Errc::unknown_topic:      return "unknown_topic";
        case Errc::unknown_partition:  return "unknown_partition";
        case Errc::offset_out_of_range: return "offset_out_of_range";
        case Errc::log_full:           return "log_full";
        case Errc::io_error:           return "io_error";
        case Errc::invalid_record:     return "invalid_record";
        case Errc::invalid_argument:   return "invalid_argument";
    }
    return "unknown";
}

template <typename T>
class [[nodiscard]] Result {
public:
    Result(T value) : storage_(std::in_place_type<T>, std::move(value)) {}
    Result(Errc error) : storage_(error) {}

    [[nodiscard]] bool ok() const noexcept { return std::holds_alternative<T>(storage_); }
    explicit operator bool() const noexcept { return ok(); }

    [[nodiscard]] T& value() & { return std::get<T>(storage_); }
    [[nodiscard]] const T& value() const& { return std::get<T>(storage_); }
    [[nodiscard]] T&& value() && { return std::get<T>(std::move(storage_)); }

    [[nodiscard]] Errc error() const noexcept {
        return ok() ? Errc::invalid_argument : std::get<Errc>(storage_);
    }

    template <typename F>
        requires std::invocable<F, const T&>
    auto map(F&& f) const -> Result<std::invoke_result_t<F, const T&>> {
        if (!ok()) {
            return error();
        }
        return std::invoke(std::forward<F>(f), value());
    }

    template <typename OnValue, typename OnError>
    decltype(auto) match(OnValue&& on_value, OnError&& on_error) const& {
        return std::visit(Overloaded{std::forward<OnValue>(on_value),
                                     std::forward<OnError>(on_error)},
                          storage_);
    }

    T value_or(T fallback) const {
        return ok() ? value() : std::move(fallback);
    }

private:
    std::variant<T, Errc> storage_;
};

using Status = Result<std::monostate>;

inline Status ok_status() { return Status{std::monostate{}}; }

}  // namespace kafka
