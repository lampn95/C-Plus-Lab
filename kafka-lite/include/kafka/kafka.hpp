#pragma once

#include "broker.hpp"
#include "consumer.hpp"
#include "producer.hpp"
#include "protocol.hpp"

// kafka-lite: in-process Kafka-style broker.
// Produce gather-writes into an mmap log; fetch returns RecordView into that mapping (zero-copy).
