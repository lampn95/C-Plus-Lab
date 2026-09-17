#include "kafka/broker.hpp"

#include "kafka/concepts.hpp"

#include <cctype>
#include <chrono>
#include <functional>
#include <sstream>

namespace kafka {
namespace {

std::string tp_key(const TopicPartition& tp) {
    std::ostringstream os;
    os << tp.topic << '#' << tp.partition;
    return os.str();
}

[[nodiscard]] bool valid_topic_name(std::string_view name) {
    if (name.empty() || name.size() > 249) {
        return false;
    }
    if (name == "." || name == "..") {
        return false;
    }
    for (unsigned char c : name) {
        if (!(std::isalnum(c) || c == '_' || c == '-' || c == '.')) {
            return false;
        }
    }
    return true;
}

}  // namespace

Broker::Broker(std::filesystem::path data_dir, std::size_t segment_size)
    : data_dir_(std::move(data_dir)), segment_size_(segment_size) {
    std::filesystem::create_directories(data_dir_);
}

Timestamp Broker::now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

Broker::Topic* Broker::find_topic(std::string_view name) {
    auto it = topics_.find(std::string(name));
    return it == topics_.end() ? nullptr : &it->second;
}

const Broker::Topic* Broker::find_topic(std::string_view name) const {
    auto it = topics_.find(std::string(name));
    return it == topics_.end() ? nullptr : &it->second;
}

std::int32_t Broker::pick_partition(Topic& topic, std::string_view key, std::int32_t requested) const {
    if (requested >= 0) {
        return requested;
    }
    if (key.empty()) {
        return static_cast<std::int32_t>(topic.round_robin % static_cast<std::uint64_t>(topic.partition_count));
    }
    const auto h = std::hash<std::string_view>{}(key);
    return static_cast<std::int32_t>(h % static_cast<std::size_t>(topic.partition_count));
}

Status Broker::create_topic(std::string_view name, std::int32_t partitions) {
    if (!valid_topic_name(name) || partitions < 1) {
        return Errc::invalid_argument;
    }
    std::lock_guard lock(mu_);
    auto [it, inserted] = topics_.try_emplace(std::string(name));
    if (!inserted) {
        return it->second.partition_count == partitions ? ok_status() : Status{Errc::invalid_argument};
    }
    auto& topic = it->second;
    topic.partition_count = partitions;
    topic.partitions.reserve(static_cast<std::size_t>(partitions));
    for (std::int32_t p = 0; p < partitions; ++p) {
        auto dir = data_dir_ / std::string(name) / std::to_string(p);
        topic.partitions.push_back(
            std::make_unique<PartitionLog>(std::move(dir), std::string(name), p, segment_size_));
    }
    return ok_status();
}

Result<ProduceAck> Broker::produce(std::string_view topic,
                                   std::string_view key,
                                   std::string_view value,
                                   std::int32_t partition) {
    std::lock_guard lock(mu_);
    auto* t = find_topic(topic);
    if (t == nullptr) {
        return Errc::unknown_topic;
    }
    const auto p = pick_partition(*t, key, partition);
    if (p < 0 || p >= t->partition_count) {
        return Errc::unknown_partition;
    }
    if (key.empty()) {
        ++t->round_robin;
    }
    auto assigned = t->partitions[static_cast<std::size_t>(p)]->append(key, value, now_ms());
    if (!assigned) {
        return assigned.error();
    }
    return ProduceAck{std::string(topic), p, assigned.value()};
}

Result<FetchResponse> Broker::fetch(std::string_view topic,
                                    std::int32_t partition,
                                    Offset offset,
                                    std::size_t max_bytes) {
    std::lock_guard lock(mu_);
    auto* t = find_topic(topic);
    if (t == nullptr) {
        return Errc::unknown_topic;
    }
    if (partition < 0 || partition >= t->partition_count) {
        return Errc::unknown_partition;
    }
    auto& log = *t->partitions[static_cast<std::size_t>(partition)];
    auto batch = log.fetch(offset, max_bytes);
    if (!batch) {
        return batch.error();
    }
    return FetchResponse{std::string(topic), partition, log.high_watermark(), std::move(batch.value())};
}

Status Broker::commit(std::string_view group, const TopicPartition& tp, Offset offset) {
    if (group.empty()) {
        return Errc::invalid_argument;
    }
    std::lock_guard lock(mu_);
    auto* t = find_topic(tp.topic);
    if (t == nullptr) {
        return Errc::unknown_topic;
    }
    if (tp.partition < 0 || tp.partition >= t->partition_count) {
        return Errc::unknown_partition;
    }
    group_offsets_[std::string(group)][tp_key(tp)] = offset;
    return ok_status();
}

Result<Offset> Broker::committed(std::string_view group, const TopicPartition& tp) const {
    std::lock_guard lock(mu_);
    auto git = group_offsets_.find(std::string(group));
    if (git == group_offsets_.end()) {
        return Offset{0};
    }
    auto oit = git->second.find(tp_key(tp));
    if (oit == git->second.end()) {
        return Offset{0};
    }
    return oit->second;
}

Result<MetadataResponse> Broker::metadata(std::string_view topic) const {
    std::lock_guard lock(mu_);
    auto* t = find_topic(topic);
    if (t == nullptr) {
        return Errc::unknown_topic;
    }
    Offset hw = 0;
    for (const auto& p : t->partitions) {
        hw += p->high_watermark();
    }
    return MetadataResponse{std::string(topic), t->partition_count, hw};
}

Result<std::int32_t> Broker::partition_count(std::string_view topic) const {
    return metadata(topic).map([](const MetadataResponse& m) { return m.partitions; });
}

Response Broker::dispatch(const Request& req) {
    return std::visit(
        Overloaded{
            [&](const ProduceRequest& r) -> Response {
                auto ack = produce(r.topic, r.key, r.value, r.partition);
                if (!ack) {
                    return ErrorResponse{ack.error(), std::string(to_string(ack.error()))};
                }
                return std::move(ack.value());
            },
            [&](const FetchRequest& r) -> Response {
                auto fr = fetch(r.topic, r.partition, r.offset, r.max_bytes);
                if (!fr) {
                    return ErrorResponse{fr.error(), std::string(to_string(fr.error()))};
                }
                return std::move(fr.value());
            },
            [&](const CreateTopicRequest& r) -> Response {
                auto st = create_topic(r.topic, r.partitions);
                if (!st) {
                    return ErrorResponse{st.error(), std::string(to_string(st.error()))};
                }
                auto md = metadata(r.topic);
                if (!md) {
                    return ErrorResponse{md.error(), std::string(to_string(md.error()))};
                }
                return std::move(md.value());
            },
            [&](const CommitOffsetRequest& r) -> Response {
                auto st = commit(r.group, r.tp, r.offset);
                if (!st) {
                    return ErrorResponse{st.error(), std::string(to_string(st.error()))};
                }
                return CommitResponse{r.tp, r.offset};
            },
            [&](const MetadataRequest& r) -> Response {
                auto md = metadata(r.topic);
                if (!md) {
                    return ErrorResponse{md.error(), std::string(to_string(md.error()))};
                }
                return std::move(md.value());
            },
        },
        req);
}

bool Broker::owns_pointer(std::string_view topic, std::int32_t partition, const void* p) const {
    std::lock_guard lock(mu_);
    auto* t = find_topic(topic);
    if (t == nullptr || partition < 0 || partition >= t->partition_count) {
        return false;
    }
    return t->partitions[static_cast<std::size_t>(partition)]->owns_pointer(p);
}

void Broker::sync() {
    std::lock_guard lock(mu_);
    for (auto& [name, topic] : topics_) {
        (void)name;
        for (auto& part : topic.partitions) {
            part->sync();
        }
    }
}

}  // namespace kafka
