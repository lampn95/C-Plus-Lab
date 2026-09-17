#pragma once

#include "buffer.hpp"
#include "error.hpp"
#include "record.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace kafka {

inline constexpr std::size_t kSegmentHeaderSize = 64;
inline constexpr std::size_t kDefaultSegmentSize = 1 << 20;  // 1 MiB

class MmapFile {
public:
    MmapFile() = default;
    MmapFile(const std::filesystem::path& path, std::size_t capacity);
    ~MmapFile();

    MmapFile(const MmapFile&) = delete;
    MmapFile& operator=(const MmapFile&) = delete;
    MmapFile(MmapFile&& other) noexcept;
    MmapFile& operator=(MmapFile&& other) noexcept;

    [[nodiscard]] bool valid() const noexcept;
    [[nodiscard]] MutableByteSpan writable() noexcept;
    [[nodiscard]] ByteSpan readable() const noexcept;
    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }
    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

    void sync();

private:
    void close() noexcept;

    std::filesystem::path path_;
    int fd_ = -1;
    void* ptr_ = nullptr;
    std::size_t capacity_ = 0;
};

class LogSegment {
public:
    LogSegment(const std::filesystem::path& path, Offset base_offset, std::size_t capacity);

    [[nodiscard]] Offset base_offset() const noexcept { return base_offset_; }
    [[nodiscard]] Offset next_offset() const noexcept { return next_offset_; }
    [[nodiscard]] std::size_t write_pos() const noexcept { return write_pos_; }
    [[nodiscard]] std::size_t remaining() const noexcept;
    [[nodiscard]] ByteSpan mapping() const noexcept { return file_.readable(); }

    Result<Offset> append(std::string_view key, std::string_view value, Timestamp ts);
    RecordBatchView fetch(Offset from, std::size_t max_bytes, std::string_view topic, std::int32_t partition) const;

    [[nodiscard]] bool contains_offset(Offset offset) const noexcept {
        return offset >= base_offset_ && offset < next_offset_;
    }

    void sync();

private:
    void load_or_init_header();
    void persist_header() noexcept;
    [[nodiscard]] bool parse_at(std::size_t pos,
                                Offset& offset,
                                Timestamp& ts,
                                std::string_view& key,
                                std::string_view& value,
                                std::size_t& record_size) const noexcept;

    MmapFile file_;
    Offset base_offset_ = 0;
    Offset next_offset_ = 0;
    std::size_t write_pos_ = kSegmentHeaderSize;
    std::vector<std::uint32_t> rec_pos_;
};

class PartitionLog {
public:
    PartitionLog(std::filesystem::path dir,
                 std::string topic,
                 std::int32_t partition,
                 std::size_t segment_size = kDefaultSegmentSize);

    Result<Offset> append(std::string_view key, std::string_view value, Timestamp ts);

    template <ByteRange K, ByteRange V>
    Result<Offset> append(const K& key, const V& value, Timestamp ts) {
        return append(as_string_view(as_bytes(key)), as_string_view(as_bytes(value)), ts);
    }

    Result<RecordBatchView> fetch(Offset from, std::size_t max_bytes) const;

    [[nodiscard]] Offset log_start() const;
    [[nodiscard]] Offset high_watermark() const;
    [[nodiscard]] bool owns_pointer(const void* p) const;
    [[nodiscard]] const std::string& topic() const noexcept { return topic_; }
    [[nodiscard]] std::int32_t partition() const noexcept { return partition_; }

    void sync();

private:
    LogSegment& active();
    const LogSegment& active() const;
    void roll();
    const LogSegment* segment_for(Offset offset) const;

    std::filesystem::path dir_;
    std::string topic_;
    std::int32_t partition_ = 0;
    std::size_t segment_size_ = kDefaultSegmentSize;
    std::vector<std::unique_ptr<LogSegment>> segments_;
};

}  // namespace kafka
