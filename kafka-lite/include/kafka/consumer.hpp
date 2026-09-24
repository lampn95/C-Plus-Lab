#pragma once

#include "broker.hpp"
#include "concepts.hpp"
#include "error.hpp"
#include "record.hpp"

#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace kafka {

class Consumer {
public:
    Consumer(Broker& broker, std::string group_id)
        : broker_(&broker), group_(std::move(group_id)) {}

    template <AllTopicNames... Topics>
        requires (sizeof...(Topics) > 0)
    Status subscribe(Topics&&... topics) {
        Status st = ok_status();
        auto one = [&](auto&& name) {
            std::string topic{std::string_view(name)};
            auto n = broker_->partition_count(topic);
            if (!n) {
                st = Status{n.error()};
                return;
            }
            for (std::int32_t p = 0; p < n.value(); ++p) {
                TopicPartition tp{topic, p};
                if (positions_.find(tp) == positions_.end()) {
                    auto committed = broker_->committed(group_, tp);
                    positions_[tp] = committed ? committed.value() : Offset{0};
                }
            }
        };
        (one(topics), ...);
        return st;
    }

    Result<std::vector<RecordView>> poll(std::size_t max_bytes = 64 * 1024) {
        std::vector<RecordView> out;
        for (auto& [tp, offset] : positions_) {
            auto fetched = broker_->fetch(tp.topic, tp.partition, offset, max_bytes);
            if (!fetched) {
                return fetched.error();
            }
            for (const auto& rec : fetched.value().batch) {
                out.push_back(rec);
                offset = rec.offset() + 1;
            }
        }
        return out;
    }

    Status commit() {
        Status st = ok_status();
        for (const auto& [tp, offset] : positions_) {
            auto s = broker_->commit(group_, tp, offset);
            if (!s) {
                st = s;
            }
        }
        return st;
    }

    [[nodiscard]] const std::string& group_id() const noexcept { return group_; }

private:
    Broker* broker_;
    std::string group_;
    std::map<TopicPartition, Offset> positions_;
};

}  // namespace kafka
