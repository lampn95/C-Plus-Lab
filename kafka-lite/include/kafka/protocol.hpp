#pragma once

#include "error.hpp"
#include "record.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace kafka {

struct ProduceRequest {
    std::string topic;
    std::int32_t partition = -1;
    std::string key;
    std::string value;
};

struct FetchRequest {
    std::string topic;
    std::int32_t partition = 0;
    Offset offset = 0;
    std::size_t max_bytes = 64 * 1024;
};

struct CreateTopicRequest {
    std::string topic;
    std::int32_t partitions = 1;
};

struct CommitOffsetRequest {
    std::string group;
    TopicPartition tp;
    Offset offset = 0;
};

struct MetadataRequest {
    std::string topic;
};

struct ProduceAck {
    std::string topic;
    std::int32_t partition = 0;
    Offset offset = -1;
};

struct FetchResponse {
    std::string topic;
    std::int32_t partition = 0;
    Offset high_watermark = 0;
    RecordBatchView batch;
};

struct MetadataResponse {
    std::string topic;
    std::int32_t partitions = 0;
    Offset high_watermark = 0;
};

struct CommitResponse {
    TopicPartition tp;
    Offset offset = 0;
};

struct ErrorResponse {
    Errc code = Errc::invalid_argument;
    std::string message;
};

using Request = std::variant<ProduceRequest,
                             FetchRequest,
                             CreateTopicRequest,
                             CommitOffsetRequest,
                             MetadataRequest>;

using Response = std::variant<ProduceAck,
                              FetchResponse,
                              MetadataResponse,
                              CommitResponse,
                              ErrorResponse>;

}  // namespace kafka
