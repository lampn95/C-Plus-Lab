#include "kafka/kafka.hpp"

#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

using namespace kafka;

namespace {

void print_response(const Response& resp) {
    std::visit(Overloaded{
                   [](const ProduceAck& ack) {
                       std::cout << "  ProduceAck topic=" << ack.topic
                                 << " partition=" << ack.partition
                                 << " offset=" << ack.offset << '\n';
                   },
                   [](const FetchResponse& fr) {
                       std::cout << "  FetchResponse records=" << fr.batch.size()
                                 << " hw=" << fr.high_watermark << '\n';
                   },
                   [](const MetadataResponse& md) {
                       std::cout << "  Metadata topic=" << md.topic
                                 << " partitions=" << md.partitions << '\n';
                   },
                   [](const CommitResponse& c) {
                       std::cout << "  Commit " << c.tp.topic << '#' << c.tp.partition
                                 << " -> " << c.offset << '\n';
                   },
                   [](const ErrorResponse& e) {
                       std::cout << "  Error " << to_string(e.code) << ": " << e.message << '\n';
                   },
               },
               resp);
}

}  // namespace

int main() {
    const auto dir = std::filesystem::path{"build"} / "demo-data";
    std::filesystem::remove_all(dir);

    Broker broker{dir};
    auto created = broker.create_topics(2, "events", "logs");
    if (!created) {
        std::cerr << "create_topics failed: " << to_string(created.error()) << '\n';
        return 1;
    }

    Producer producer{broker};
    const std::string payload = "zero-copy-payload";

    auto a1 = producer.send("events", "user-1", payload);
    auto a2 = producer.send("events", Record{"user-2", "hello-from-record"});
    auto a3 = producer.send_all("events",
                                Record{"batch-a", "one"},
                                Record{"batch-b", "two"},
                                Record{"batch-c", "three"});
    if (!a1 || !a2 || !a3) {
        std::cerr << "produce failed\n";
        return 1;
    }

    std::cout << "produced offsets: " << a1.value().offset << ", "
              << a2.value().offset << ", last-batch=" << a3.value().offset << '\n';

    Request create = CreateTopicRequest{"metrics", 1};
    print_response(broker.dispatch(create));

    Request produce_msg = ProduceRequest{"logs", -1, "k", "via-variant"};
    print_response(broker.dispatch(produce_msg));

    Consumer consumer{broker, "group-analytics"};
    auto sub = consumer.subscribe("events", "logs");
    if (!sub) {
        std::cerr << "subscribe failed: " << to_string(sub.error()) << '\n';
        return 1;
    }

    auto polled = consumer.poll();
    if (!polled) {
        std::cerr << "poll failed: " << to_string(polled.error()) << '\n';
        return 1;
    }

    std::cout << "consumed " << polled.value().size() << " records:\n";
    int zero_copy = 0;
    for (const auto& rec : polled.value()) {
        const bool zc = broker.owns_pointer(rec.topic(), rec.partition(), rec.value().data()) ||
                        rec.value().empty();
        zero_copy += zc ? 1 : 0;
        std::cout << "  " << rec.topic() << '-' << rec.partition() << '@' << rec.offset()
                  << " key=" << rec.key() << " value=" << rec.value()
                  << " ptr=" << static_cast<const void*>(rec.value().data())
                  << (zc ? "  [mmap zero-copy]" : "  [copied]") << '\n';
    }
    std::cout << "zero-copy records: " << zero_copy << '/' << polled.value().size() << '\n';

    if (auto c = consumer.commit(); !c) {
        std::cerr << "commit failed\n";
        return 1;
    }

    // Protocol-level fetch should alias the same mmap pages.
    Request fetch = FetchRequest{"events", a1.value().partition, a1.value().offset, 4096};
    auto fetched = broker.dispatch(fetch);
    std::visit(Overloaded{
                   [&](const FetchResponse& fr) {
                       if (fr.batch.empty()) {
                           std::cerr << "expected records in fetch\n";
                           return;
                       }
                       const auto& rec = *fr.batch.begin();
                       std::cout << "fetch view aliases mmap: "
                                 << std::boolalpha
                                 << fr.batch.is_zero_copy(rec) << '\n';
                   },
                   [](const auto&) { std::cout << "unexpected fetch response\n"; },
               },
               fetched);

    broker.sync();
    return 0;
}
