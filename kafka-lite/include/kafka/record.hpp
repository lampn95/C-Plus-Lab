#pragma once

#include "buffer.hpp"
#include "concepts.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <compare>

namespace kafka {

using Offset = std::int64_t;
using Timestamp = std::int64_t;

struct TopicPartition {
    std::string topic;
    std::int32_t partition = 0;

    auto operator<=>(const TopicPartition&) const = default;
};

// Owned record (producer / tests). Views below never copy payload.
class Record {
public:
    Record() = default;
    Record(std::string key, std::string value, Timestamp ts = 0)
        : key_(std::move(key)), value_(std::move(value)), timestamp_(ts) {}

    [[nodiscard]] std::string_view key() const noexcept { return key_; }
    [[nodiscard]] std::string_view value() const noexcept { return value_; }
    [[nodiscard]] Timestamp timestamp() const noexcept { return timestamp_; }

private:
    std::string key_;
    std::string value_;
    Timestamp timestamp_ = 0;
};

// Zero-copy view: key/value point into the mmap'd log (or a fetch buffer).
class RecordView {
public:
    RecordView() = default;

    RecordView(std::string_view topic,
               std::int32_t partition,
               Offset offset,
               Timestamp timestamp,
               std::string_view key,
               std::string_view value) noexcept
        : topic_(topic),
          partition_(partition),
          offset_(offset),
          timestamp_(timestamp),
          key_(key),
          value_(value) {}

    [[nodiscard]] std::string_view topic() const noexcept { return topic_; }
    [[nodiscard]] std::int32_t partition() const noexcept { return partition_; }
    [[nodiscard]] Offset offset() const noexcept { return offset_; }
    [[nodiscard]] Timestamp timestamp() const noexcept { return timestamp_; }
    [[nodiscard]] std::string_view key() const noexcept { return key_; }
    [[nodiscard]] std::string_view value() const noexcept { return value_; }

    [[nodiscard]] ByteSpan key_bytes() const noexcept { return as_bytes(key_); }
    [[nodiscard]] ByteSpan value_bytes() const noexcept { return as_bytes(value_); }

    [[nodiscard]] Record to_owned() const { return Record{std::string(key_), std::string(value_), timestamp_}; }

private:
    std::string_view topic_{};
    std::int32_t partition_ = 0;
    Offset offset_ = -1;
    Timestamp timestamp_ = 0;
    std::string_view key_{};
    std::string_view value_{};
};

static_assert(RecordLike<Record>);
static_assert(RecordLike<RecordView>);

class RecordBatchView {
public:
    RecordBatchView() = default;
    explicit RecordBatchView(std::vector<RecordView> records, ByteSpan mmap_region = {})
        : records_(std::move(records)), mmap_region_(mmap_region) {}

    [[nodiscard]] const std::vector<RecordView>& records() const noexcept { return records_; }
    [[nodiscard]] std::size_t size() const noexcept { return records_.size(); }
    [[nodiscard]] bool empty() const noexcept { return records_.empty(); }
    [[nodiscard]] ByteSpan mmap_region() const noexcept { return mmap_region_; }

    [[nodiscard]] auto begin() const noexcept { return records_.begin(); }
    [[nodiscard]] auto end() const noexcept { return records_.end(); }

    // True if payload still aliases the mmap (no userspace copy).
    [[nodiscard]] bool is_zero_copy(const RecordView& r) const noexcept {
        return pointer_in_span(r.value().data(), mmap_region_) ||
               pointer_in_span(r.key().data(), mmap_region_) ||
               (r.value().empty() && r.key().empty());
    }

    template <RecordHandler<RecordView> F>
    void for_each(F&& f) const {
        for (const auto& rec : records_) {
            std::forward<F>(f)(rec);
        }
    }

private:
    std::vector<RecordView> records_;
    ByteSpan mmap_region_{};
};

}  // namespace kafka
