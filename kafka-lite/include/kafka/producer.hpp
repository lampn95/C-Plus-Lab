#pragma once

#include "broker.hpp"
#include "concepts.hpp"
#include "error.hpp"
#include "record.hpp"

#include <string>
#include <string_view>
#include <utility>

namespace kafka {

class Producer {
public:
    explicit Producer(Broker& broker) : broker_(&broker) {}

    Result<ProduceAck> send(std::string_view topic,
                            std::string_view key,
                            std::string_view value,
                            std::int32_t partition = -1) {
        return broker_->produce(topic, key, value, partition);
    }

    template <ByteRange K, ByteRange V>
    Result<ProduceAck> send(std::string_view topic,
                            const K& key,
                            const V& value,
                            std::int32_t partition = -1) {
        return broker_->produce(topic, key, value, partition);
    }

    template <AllRecordLike... Recs>
        requires (sizeof...(Recs) > 0)
    Result<ProduceAck> send_all(std::string_view topic, Recs&&... recs) {
        return broker_->produce_all(topic, std::forward<Recs>(recs)...);
    }

    Result<ProduceAck> send(std::string_view topic,
                            const RecordLike auto& rec,
                            std::int32_t partition = -1) {
        return broker_->produce(topic, rec.key(), rec.value(), partition);
    }

private:
    Broker* broker_;
};

}  // namespace kafka
