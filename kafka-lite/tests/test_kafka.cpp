#include "kafka/kafka.hpp"

#include <cstddef>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

using namespace kafka;

namespace {

struct Checks {
    int passed = 0;
    int failed = 0;

    void expect(bool cond, const char* expr, const char* file, int line) {
        if (cond) {
            ++passed;
            return;
        }
        ++failed;
        std::cerr << "FAIL " << file << ':' << line << "  " << expr << '\n';
    }
};

Checks g_checks;

#define EXPECT(cond) g_checks.expect(static_cast<bool>(cond), #cond, __FILE__, __LINE__)

std::filesystem::path make_dir(std::string_view name) {
    auto dir = std::filesystem::path{"build"} / name;
    std::filesystem::remove_all(dir);
    return dir;
}

int count_logs(const std::filesystem::path& partition_dir) {
    int n = 0;
    if (!std::filesystem::exists(partition_dir)) {
        return 0;
    }
    for (const auto& entry : std::filesystem::directory_iterator(partition_dir)) {
        if (entry.path().extension() == ".log") {
            ++n;
        }
    }
    return n;
}

void test_basic_produce_fetch() {
    Broker broker{make_dir("kafka-lite-test-basic"), 64 * 1024};
    EXPECT(broker.create_topic("t", 2));
    Producer p{broker};
    auto ack = p.send("t", "k", "v");
    EXPECT(ack);
    EXPECT(ack.value().offset == 0);

    auto batch = broker.fetch("t", ack.value().partition, 0, 4096);
    EXPECT(batch);
    EXPECT(batch.value().batch.size() == 1);
    const auto& rec = *batch.value().batch.begin();
    EXPECT(rec.key() == "k");
    EXPECT(rec.value() == "v");
    EXPECT(rec.timestamp() > 0);
    EXPECT(batch.value().batch.is_zero_copy(rec));
    EXPECT(broker.owns_pointer("t", rec.partition(), rec.value().data()));
}

void test_fold_and_consumer() {
    Broker broker{make_dir("kafka-lite-test-fold"), 64 * 1024};
    EXPECT(broker.create_topics(1, "a", "b"));
    EXPECT(broker.produce_all("a", Record{"1", "x"}, Record{"2", "y"}));
    Consumer c{broker, "g"};
    EXPECT(c.subscribe("a"));
    auto recs = c.poll();
    EXPECT(recs);
    EXPECT(recs.value().size() == 2);
    EXPECT(c.commit());

    auto again = c.poll();
    EXPECT(again);
    EXPECT(again.value().empty());

    Consumer c2{broker, "g"};
    EXPECT(c2.subscribe("a"));
    auto resumed = c2.poll();
    EXPECT(resumed);
    EXPECT(resumed.value().empty());
}

void test_errors_and_validation() {
    Broker broker{make_dir("kafka-lite-test-errors"), 64 * 1024};
    auto unknown = broker.dispatch(ProduceRequest{"nope", -1, "k", "v"});
    EXPECT(std::holds_alternative<ErrorResponse>(unknown));
    EXPECT(std::get<ErrorResponse>(unknown).code == Errc::unknown_topic);

    EXPECT(!broker.create_topic("", 1));
    EXPECT(!broker.create_topic("../escape", 1));
    EXPECT(!broker.create_topic("bad/name", 1));
    EXPECT(!broker.create_topic("ok", 0));
    EXPECT(broker.create_topic("ok", 2));
    EXPECT(broker.create_topic("ok", 2));
    EXPECT(!broker.create_topic("ok", 3));

    auto bad_part = broker.produce("ok", "k", "v", 99);
    EXPECT(!bad_part);
    EXPECT(bad_part.error() == Errc::unknown_partition);

    EXPECT(broker.produce("ok", "k", "v", 0));
    auto oor = broker.fetch("ok", 0, 99, 1024);
    EXPECT(!oor);
    EXPECT(oor.error() == Errc::offset_out_of_range);

    auto eof = broker.fetch("ok", 0, 1, 1024);
    EXPECT(eof);
    EXPECT(eof.value().batch.empty());

    Consumer missing{broker, "g"};
    EXPECT(!missing.subscribe("nope"));
}

void test_persist_reopen() {
    const auto dir = make_dir("kafka-lite-test-persist");
    Offset saved = -1;
    std::int32_t part = 0;
    {
        Broker broker{dir, 64 * 1024};
        EXPECT(broker.create_topic("persist", 1));
        auto ack = broker.produce("persist", "k", "durable");
        EXPECT(ack);
        saved = ack.value().offset;
        part = ack.value().partition;
        broker.sync();
    }
    Broker broker{dir, 64 * 1024};
    EXPECT(broker.create_topic("persist", 1));
    auto fr = broker.fetch("persist", part, saved, 4096);
    EXPECT(fr);
    EXPECT(fr.value().batch.size() == 1);
    EXPECT(fr.value().batch.begin()->value() == "durable");
}

void test_partitioning() {
    Broker broker{make_dir("kafka-lite-test-part"), 64 * 1024};
    EXPECT(broker.create_topic("p", 2));

    auto a = broker.produce("p", "same-key", "1");
    auto b = broker.produce("p", "same-key", "2");
    EXPECT(a && b);
    EXPECT(a.value().partition == b.value().partition);
    EXPECT(b.value().offset == a.value().offset + 1);

    auto p0 = broker.produce("p", "k", "forced-0", 0);
    auto p1 = broker.produce("p", "k", "forced-1", 1);
    EXPECT(p0 && p1);
    EXPECT(p0.value().partition == 0);
    EXPECT(p1.value().partition == 1);

    auto r0 = broker.produce("p", "", "rr0");
    auto r1 = broker.produce("p", "", "rr1");
    EXPECT(r0 && r1);
    EXPECT(r0.value().partition != r1.value().partition);
}

void test_empty_and_binary() {
    Broker broker{make_dir("kafka-lite-test-binary"), 64 * 1024};
    EXPECT(broker.create_topic("bin", 1));

    EXPECT(broker.produce("bin", "", ""));
    std::string raw{"hel\0lo", 6};
    auto ack = broker.produce("bin", "k", raw);
    EXPECT(ack);

    std::vector<char> key{'b', 'y'};
    std::vector<std::byte> val{std::byte{0x00}, std::byte{0xff}};
    Producer prod{broker};
    EXPECT(prod.send("bin", key, val));

    auto fr = broker.fetch("bin", 0, 0, 4096);
    EXPECT(fr);
    EXPECT(fr.value().batch.size() == 3);
    auto it = fr.value().batch.begin();
    EXPECT(it->key().empty());
    EXPECT(it->value().empty());
    ++it;
    EXPECT(it->value().size() == 6);
    EXPECT(it->value() == raw);
    ++it;
    EXPECT(it->key() == "by");
    EXPECT(it->value().size() == 2);
    EXPECT(static_cast<unsigned char>(it->value()[1]) == 0xff);
}

void test_max_bytes() {
    Broker broker{make_dir("kafka-lite-test-max-bytes"), 64 * 1024};
    EXPECT(broker.create_topic("m", 1));
    const std::string payload(64, 'x');
    EXPECT(broker.produce("m", "a", payload, 0));
    EXPECT(broker.produce("m", "b", payload, 0));

    auto small = broker.fetch("m", 0, 0, 80);
    EXPECT(small);
    EXPECT(small.value().batch.size() == 1);
    EXPECT(small.value().batch.begin()->key() == "a");

    auto big = broker.fetch("m", 0, 0, 4096);
    EXPECT(big);
    EXPECT(big.value().batch.size() == 2);
}

void test_segment_roll() {
    constexpr std::size_t seg = 256;
    Broker broker{make_dir("kafka-lite-test-roll"), seg};
    EXPECT(broker.create_topic("roll", 1));

    const std::string payload(80, 'z');
    constexpr int n = 8;
    for (int i = 0; i < n; ++i) {
        auto ack = broker.produce("roll", "k", payload, 0);
        EXPECT(ack);
        EXPECT(ack.value().offset == i);
    }
    EXPECT(count_logs(broker.data_dir() / "roll" / "0") >= 2);

    for (int i = 0; i < n; ++i) {
        auto fr = broker.fetch("roll", 0, i, 4096);
        EXPECT(fr);
        EXPECT(!fr.value().batch.empty());
        EXPECT(fr.value().batch.begin()->offset() == i);
        EXPECT(fr.value().batch.begin()->value() == payload);
        EXPECT(fr.value().batch.is_zero_copy(*fr.value().batch.begin()));
    }

    Consumer c{broker, "roll-group"};
    EXPECT(c.subscribe("roll"));
    int seen = 0;
    for (int round = 0; round < 16; ++round) {
        auto recs = c.poll(128);
        EXPECT(recs);
        if (recs.value().empty()) {
            break;
        }
        seen += static_cast<int>(recs.value().size());
    }
    EXPECT(seen == n);
}

void test_protocol_variant() {
    Broker broker{make_dir("kafka-lite-test-protocol"), 64 * 1024};
    auto created = broker.dispatch(CreateTopicRequest{"events", 1});
    EXPECT(std::holds_alternative<MetadataResponse>(created));
    EXPECT(std::get<MetadataResponse>(created).partitions == 1);

    auto produced = broker.dispatch(ProduceRequest{"events", 0, "k", "v"});
    EXPECT(std::holds_alternative<ProduceAck>(produced));

    auto fetched = broker.dispatch(FetchRequest{"events", 0, 0, 4096});
    EXPECT(std::holds_alternative<FetchResponse>(fetched));
    EXPECT(std::get<FetchResponse>(fetched).batch.size() == 1);

    auto committed = broker.dispatch(CommitOffsetRequest{"g", TopicPartition{"events", 0}, 1});
    EXPECT(std::holds_alternative<CommitResponse>(committed));

    auto md = broker.dispatch(MetadataRequest{"events"});
    EXPECT(std::holds_alternative<MetadataResponse>(md));

    auto n = broker.partition_count("events");
    EXPECT(n && n.value() == 1);
    n.match([](std::int32_t parts) { EXPECT(parts == 1); },
            [](Errc) { EXPECT(false); });
}

}  // namespace

int main() {
    test_basic_produce_fetch();
    test_fold_and_consumer();
    test_errors_and_validation();
    test_persist_reopen();
    test_partitioning();
    test_empty_and_binary();
    test_max_bytes();
    test_segment_roll();
    test_protocol_variant();

    std::cout << "kafka-lite tests: " << g_checks.passed << " passed, "
              << g_checks.failed << " failed\n";
    return g_checks.failed == 0 ? 0 : 1;
}
