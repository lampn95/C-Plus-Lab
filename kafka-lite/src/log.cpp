#include "kafka/log.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

namespace kafka {
namespace {

constexpr char kMagic[4] = {'K', 'L', 'G', '1'};

void* map_failed() { return MAP_FAILED; }

}  // namespace

MmapFile::MmapFile(const std::filesystem::path& path, std::size_t capacity)
    : path_(path), capacity_(capacity) {
    if (capacity_ < kSegmentHeaderSize) {
        throw std::runtime_error("mmap capacity too small");
    }
    std::filesystem::create_directories(path_.parent_path());

    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("open log segment failed");
    }

    struct stat st {};
    if (::fstat(fd_, &st) != 0) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("fstat failed");
    }
    if (static_cast<std::size_t>(st.st_size) < capacity_) {
        if (::ftruncate(fd_, static_cast<off_t>(capacity_)) != 0) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("ftruncate failed");
        }
    }

    ptr_ = ::mmap(nullptr, capacity_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (ptr_ == map_failed()) {
        ptr_ = nullptr;
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("mmap failed");
    }
}

MmapFile::~MmapFile() { close(); }

MmapFile::MmapFile(MmapFile&& other) noexcept
    : path_(std::move(other.path_)),
      fd_(std::exchange(other.fd_, -1)),
      ptr_(std::exchange(other.ptr_, nullptr)),
      capacity_(std::exchange(other.capacity_, 0)) {}

MmapFile& MmapFile::operator=(MmapFile&& other) noexcept {
    if (this != &other) {
        close();
        path_ = std::move(other.path_);
        fd_ = std::exchange(other.fd_, -1);
        ptr_ = std::exchange(other.ptr_, nullptr);
        capacity_ = std::exchange(other.capacity_, 0);
    }
    return *this;
}

bool MmapFile::valid() const noexcept { return ptr_ != nullptr && fd_ >= 0; }

MutableByteSpan MmapFile::writable() noexcept {
    return {static_cast<std::byte*>(ptr_), capacity_};
}

ByteSpan MmapFile::readable() const noexcept {
    return {static_cast<const std::byte*>(ptr_), capacity_};
}

void MmapFile::sync() {
    if (ptr_ != nullptr) {
        ::msync(ptr_, capacity_, MS_SYNC);
    }
}

void MmapFile::close() noexcept {
    if (ptr_ != nullptr) {
        ::msync(ptr_, capacity_, MS_SYNC);
        ::munmap(ptr_, capacity_);
        ptr_ = nullptr;
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    capacity_ = 0;
}

LogSegment::LogSegment(const std::filesystem::path& path, Offset base_offset, std::size_t capacity)
    : file_(path, capacity), base_offset_(base_offset), next_offset_(base_offset) {
    load_or_init_header();
}

std::size_t LogSegment::remaining() const noexcept {
    const auto cap = file_.capacity();
    return cap > write_pos_ ? cap - write_pos_ : 0;
}

void LogSegment::load_or_init_header() {
    auto mem = file_.writable();
    const bool fresh = std::memcmp(mem.data(), kMagic, 4) != 0;
    if (fresh) {
        write_pos_ = kSegmentHeaderSize;
        next_offset_ = base_offset_;
        persist_header();
        return;
    }

    base_offset_ = static_cast<Offset>(load_le64(mem.data() + 8));
    write_pos_ = load_le32(mem.data() + 16);
    const auto count = load_le32(mem.data() + 20);
    next_offset_ = base_offset_;
    rec_pos_.clear();
    rec_pos_.reserve(count);

    std::size_t pos = kSegmentHeaderSize;
    for (std::uint32_t i = 0; i < count; ++i) {
        Offset off = 0;
        Timestamp ts = 0;
        std::string_view key;
        std::string_view value;
        std::size_t rec_size = 0;
        if (!parse_at(pos, off, ts, key, value, rec_size)) {
            break;
        }
        rec_pos_.push_back(static_cast<std::uint32_t>(pos));
        ++next_offset_;
        pos += rec_size;
    }
    write_pos_ = pos;
    persist_header();
}

void LogSegment::persist_header() noexcept {
    auto mem = file_.writable();
    std::memcpy(mem.data(), kMagic, 4);
    store_le32(mem.data() + 4, 1);
    store_le64(mem.data() + 8, static_cast<std::uint64_t>(base_offset_));
    store_le32(mem.data() + 16, static_cast<std::uint32_t>(write_pos_));
    store_le32(mem.data() + 20, static_cast<std::uint32_t>(rec_pos_.size()));
}

bool LogSegment::parse_at(std::size_t pos,
                          Offset& offset,
                          Timestamp& ts,
                          std::string_view& key,
                          std::string_view& value,
                          std::size_t& record_size) const noexcept {
    auto mem = file_.readable();
    if (pos + 4 > mem.size()) {
        return false;
    }
    const auto payload = load_le32(mem.data() + pos);
    record_size = 4 + payload;
    if (pos + record_size > mem.size() || payload < 24) {
        return false;
    }
    const std::byte* p = mem.data() + pos + 4;
    offset = static_cast<Offset>(load_le64(p));
    ts = static_cast<Timestamp>(load_le64(p + 8));
    const auto klen = load_le32(p + 16);
    if (24 + klen > payload) {
        return false;
    }
    key = std::string_view{reinterpret_cast<const char*>(p + 20), klen};
    const auto vlen = load_le32(p + 20 + klen);
    if (24 + klen + vlen > payload) {
        return false;
    }
    value = std::string_view{reinterpret_cast<const char*>(p + 24 + klen), vlen};
    return true;
}

Result<Offset> LogSegment::append(std::string_view key, std::string_view value, Timestamp ts) {
    if (key.size() > 16 * 1024 * 1024 || value.size() > 16 * 1024 * 1024) {
        return Errc::invalid_record;
    }

    const std::uint32_t klen = static_cast<std::uint32_t>(key.size());
    const std::uint32_t vlen = static_cast<std::uint32_t>(value.size());
    const std::uint32_t payload = 8 + 8 + 4 + klen + 4 + vlen;
    const std::size_t rec_size = 4 + payload;
    if (rec_size > remaining()) {
        return Errc::log_full;
    }

    auto mem = file_.writable();
    std::byte* dst = mem.data() + write_pos_;
    store_le32(dst, payload);
    store_le64(dst + 4, static_cast<std::uint64_t>(next_offset_));
    store_le64(dst + 12, static_cast<std::uint64_t>(ts));
    store_le32(dst + 20, klen);

    std::byte header_vlen[4];
    store_le32(header_vlen, vlen);

    // Gather-write: header fragments + key/value views, no concatenated temp buffer.
    const auto written = GatherList::gather_copy(
        MutableByteSpan{dst + 24, rec_size - 24},
        key,
        std::span<const std::byte>{header_vlen, 4},
        value);
    if (written != rec_size - 24) {
        return Errc::io_error;
    }

    rec_pos_.push_back(static_cast<std::uint32_t>(write_pos_));
    const Offset assigned = next_offset_;
    ++next_offset_;
    write_pos_ += rec_size;
    persist_header();
    return assigned;
}

RecordBatchView LogSegment::fetch(Offset from,
                                  std::size_t max_bytes,
                                  std::string_view topic,
                                  std::int32_t partition) const {
    std::vector<RecordView> out;
    if (!contains_offset(from) && from != next_offset_) {
        return RecordBatchView{std::move(out), file_.readable()};
    }
    if (from >= next_offset_ || rec_pos_.empty()) {
        return RecordBatchView{std::move(out), file_.readable()};
    }

    const std::size_t idx = static_cast<std::size_t>(from - base_offset_);
    std::size_t bytes = 0;
    for (std::size_t i = idx; i < rec_pos_.size(); ++i) {
        Offset off = 0;
        Timestamp ts = 0;
        std::string_view key;
        std::string_view value;
        std::size_t rec_size = 0;
        if (!parse_at(rec_pos_[i], off, ts, key, value, rec_size)) {
            break;
        }
        if (!out.empty() && bytes + rec_size > max_bytes) {
            break;
        }
        out.emplace_back(topic, partition, off, ts, key, value);
        bytes += rec_size;
    }
    return RecordBatchView{std::move(out), file_.readable()};
}

void LogSegment::sync() {
    persist_header();
    file_.sync();
}

PartitionLog::PartitionLog(std::filesystem::path dir,
                           std::string topic,
                           std::int32_t partition,
                           std::size_t segment_size)
    : dir_(std::move(dir)),
      topic_(std::move(topic)),
      partition_(partition),
      segment_size_(segment_size) {
    std::filesystem::create_directories(dir_);

    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::directory_iterator(dir_)) {
        if (entry.path().extension() == ".log") {
            files.push_back(entry.path());
        }
    }
    std::sort(files.begin(), files.end());

    if (files.empty()) {
        const auto path = dir_ / "00000000000000000000.log";
        segments_.push_back(std::make_unique<LogSegment>(path, Offset{0}, segment_size_));
        return;
    }

    for (const auto& path : files) {
        const auto stem = path.stem().string();
        Offset base = 0;
        try {
            base = static_cast<Offset>(std::stoll(stem));
        } catch (...) {
            base = 0;
        }
        segments_.push_back(std::make_unique<LogSegment>(path, base, segment_size_));
    }
}

LogSegment& PartitionLog::active() { return *segments_.back(); }
const LogSegment& PartitionLog::active() const { return *segments_.back(); }

void PartitionLog::roll() {
    const Offset next = active().next_offset();
    char name[32];
    std::snprintf(name, sizeof(name), "%020lld.log", static_cast<long long>(next));
    segments_.push_back(std::make_unique<LogSegment>(dir_ / name, next, segment_size_));
}

Result<Offset> PartitionLog::append(std::string_view key, std::string_view value, Timestamp ts) {
    auto result = active().append(key, value, ts);
    if (!result && result.error() == Errc::log_full) {
        roll();
        result = active().append(key, value, ts);
    }
    return result;
}

const LogSegment* PartitionLog::segment_for(Offset offset) const {
    for (const auto& seg : segments_) {
        if (seg->contains_offset(offset)) {
            return seg.get();
        }
    }
    return nullptr;
}

Result<RecordBatchView> PartitionLog::fetch(Offset from, std::size_t max_bytes) const {
    if (from < log_start() || from > high_watermark()) {
        return Errc::offset_out_of_range;
    }
    if (from == high_watermark()) {
        return RecordBatchView{};
    }
    const auto* seg = segment_for(from);
    if (seg == nullptr) {
        return Errc::offset_out_of_range;
    }
    return seg->fetch(from, max_bytes, topic_, partition_);
}

Offset PartitionLog::log_start() const {
    return segments_.empty() ? 0 : segments_.front()->base_offset();
}

Offset PartitionLog::high_watermark() const {
    return segments_.empty() ? 0 : segments_.back()->next_offset();
}

bool PartitionLog::owns_pointer(const void* p) const {
    for (const auto& seg : segments_) {
        if (pointer_in_span(p, seg->mapping())) {
            return true;
        }
    }
    return false;
}

void PartitionLog::sync() {
    for (auto& seg : segments_) {
        seg->sync();
    }
}

}  // namespace kafka
