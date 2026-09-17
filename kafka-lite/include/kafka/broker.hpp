#pragma once

#include "concepts.hpp"
#include "error.hpp"
#include "log.hpp"
#include "protocol.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace kafka {

class Broker {
public:
    explicit Broker(std::filesystem::path data_dir,
                    std::size_t segment_size = kDefaultSegmentSize);

    Status create_topic(std::string_view name, std::int32_t partitions);

    template <AllTopicNames... Names>
        requires (sizeof...(Names) > 0)
    Status create_topics(std::int32_t partitions, Names&&... names);

    template <ByteRange K, ByteRange V>
    Result<ProduceAck> produce(std::string_view topic,
                               const K& key,
                               const V& value,
                               std::int32_t partition = -1);

    Result<ProduceAck> produce(std::string_view topic,
                               std::string_view key,
                               std::string_view value,
                               std::int32_t partition = -1);

    template <AllRecordLike... Recs>
        requires (sizeof...(Recs) > 0)
    Result<ProduceAck> produce_all(std::string_view topic, Recs&&... recs);

    Result<FetchResponse> fetch(std::string_view topic,
                                std::int32_t partition,
                                Offset offset,
                                std::size_t max_bytes = 64 * 1024);

    Status commit(std::string_view group, const TopicPartition& tp, Offset offset);
    Result<Offset> committed(std::string_view group, const TopicPartition& tp) const;

    Result<MetadataResponse> metadata(std::string_view topic) const;
    Result<std::int32_t> partition_count(std::string_view topic) const;

    Response dispatch(const Request& req);

    [[nodiscard]] bool owns_pointer(std::string_view topic, std::int32_t partition, const void* p) const;
    [[nodiscard]] const std::filesystem::path& data_dir() const noexcept { return data_dir_; }
    void sync();

private:
    struct Topic {
        std::int32_t partition_count = 0;
        std::vector<std::unique_ptr<PartitionLog>> partitions;
        std::uint64_t round_robin = 0;
    };

    Topic* find_topic(std::string_view name);
    const Topic* find_topic(std::string_view name) const;
    std::int32_t pick_partition(Topic& topic, std::string_view key, std::int32_t requested) const;
    static Timestamp now_ms();

    std::filesystem::path data_dir_;
    std::size_t segment_size_;
    mutable std::mutex mu_;
    std::unordered_map<std::string, Topic> topics_;
    std::unordered_map<std::string, std::unordered_map<std::string, Offset>> group_offsets_;
};

template <AllTopicNames... Names>
    requires (sizeof...(Names) > 0)
Status Broker::create_topics(std::int32_t partitions, Names&&... names) {
    Status st = ok_status();
    auto one = [&](auto&& name) {
        auto s = create_topic(std::string_view(name), partitions);
        if (!s) {
            st = s;
        }
    };
    (one(names), ...);
    return st;
}

template <ByteRange K, ByteRange V>
Result<ProduceAck> Broker::produce(std::string_view topic,
                                   const K& key,
                                   const V& value,
                                   std::int32_t partition) {
    return produce(topic, as_string_view(as_bytes(key)), as_string_view(as_bytes(value)), partition);
}

template <AllRecordLike... Recs>
    requires (sizeof...(Recs) > 0)
Result<ProduceAck> Broker::produce_all(std::string_view topic, Recs&&... recs) {
    Result<ProduceAck> last{Errc::invalid_argument};
    bool ok = true;
    auto send_one = [&](const auto& rec) {
        if (!ok) {
            return;
        }
        last = produce(topic, rec.key(), rec.value());
        ok = static_cast<bool>(last);
    };
    (send_one(recs), ...);
    return last;
}

}  // namespace kafka
