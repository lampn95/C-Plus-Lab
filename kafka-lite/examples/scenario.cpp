#include "kafka/kafka.hpp"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

using namespace kafka;

namespace {

int g_step = 0;
int g_failed = 0;

void step(std::string_view title) {
    ++g_step;
    std::cout << "\n== " << g_step << ". " << title << " ==\n";
}

void fail(std::string_view msg) {
    ++g_failed;
    std::cerr << "  FAIL: " << msg << '\n';
}

void ok(std::string_view msg) { std::cout << "  " << msg << '\n'; }

void show(const RecordView& rec, const Broker& broker) {
    const bool zc = rec.value().empty() ||
                    broker.owns_pointer(rec.topic(), rec.partition(), rec.value().data());
    std::cout << "    " << rec.topic() << '-' << rec.partition() << '@' << rec.offset()
              << "  key=" << rec.key() << "  value=" << rec.value()
              << (zc ? "  [mmap]" : "  [copy]") << '\n';
}

Result<std::vector<RecordView>> drain(Consumer& c) {
    std::vector<RecordView> all;
    for (int i = 0; i < 32; ++i) {
        auto batch = c.poll();
        if (!batch) {
            return batch.error();
        }
        if (batch.value().empty()) {
            break;
        }
        all.insert(all.end(), batch.value().begin(), batch.value().end());
    }
    return all;
}

}  // namespace

int main() {
    const auto dir = std::filesystem::path{"build"} / "scenario-data";
    std::filesystem::remove_all(dir);

    std::cout << "kafka-lite scenario: checkout -> inventory -> analytics -> persist\n";
    std::cout << "data dir: " << dir << '\n';

    step("Broker starts, create topic orders (2 partitions)");
    Broker broker{dir};
    if (!broker.create_topic("orders", 2)) {
        fail("create_topic orders");
        return 1;
    }
    ok("topic orders ready");

    step("Checkout service produces 3 orders (key = user id -> same user, same partition)");
    Producer checkout{broker};
    const auto o1 = checkout.send("orders", "user-42", "order:coffee");
    const auto o2 = checkout.send("orders", "user-7", "order:tea");
    const auto o3 = checkout.send("orders", "user-42", "order:bagel");
    if (!o1 || !o2 || !o3) {
        fail("produce");
        return 1;
    }
    ok("user-42 coffee  -> p" + std::to_string(o1.value().partition) +
       " @" + std::to_string(o1.value().offset));
    ok("user-7  tea     -> p" + std::to_string(o2.value().partition) +
       " @" + std::to_string(o2.value().offset));
    ok("user-42 bagel   -> p" + std::to_string(o3.value().partition) +
       " @" + std::to_string(o3.value().offset));
    if (o1.value().partition != o3.value().partition) {
        fail("same key must land on same partition");
    } else {
        ok("user-42 stayed on one partition");
    }

    step("Inventory consumer group reads the backlog and commits");
    Consumer inventory{broker, "group-inventory"};
    if (!inventory.subscribe("orders")) {
        fail("inventory subscribe");
        return 1;
    }
    auto inv1 = drain(inventory);
    if (!inv1) {
        fail("inventory poll");
        return 1;
    }
    ok("inventory saw " + std::to_string(inv1.value().size()) + " records:");
    for (const auto& rec : inv1.value()) {
        show(rec, broker);
    }
    if (inv1.value().size() != 3) {
        fail("inventory should see 3 orders");
    }
    if (!inventory.commit()) {
        fail("inventory commit");
        return 1;
    }
    ok("committed group-inventory offsets");

    step("Inventory polls again — should be idle (no new orders)");
    auto inv_idle = drain(inventory);
    if (!inv_idle || !inv_idle.value().empty()) {
        fail("inventory expected empty poll");
    } else {
        ok("no more records for group-inventory");
    }

    step("Analytics is a different group — it reads the same log from offset 0");
    Consumer analytics{broker, "group-analytics"};
    if (!analytics.subscribe("orders")) {
        fail("analytics subscribe");
        return 1;
    }
    auto an = drain(analytics);
    if (!an) {
        fail("analytics poll");
        return 1;
    }
    ok("analytics saw " + std::to_string(an.value().size()) + " records (independent offsets)");
    if (an.value().size() != 3) {
        fail("analytics should also see 3 orders");
    }
    if (!analytics.commit()) {
        fail("analytics commit");
    }

    step("Checkout produces one more order; only inventory should see the delta");
    const auto o4 = checkout.send("orders", "user-7", "order:cookie");
    if (!o4) {
        fail("produce cookie");
        return 1;
    }
    ok("user-7 cookie -> p" + std::to_string(o4.value().partition) +
       " @" + std::to_string(o4.value().offset));

    auto inv2 = drain(inventory);
    if (!inv2) {
        fail("inventory delta poll");
        return 1;
    }
    ok("inventory delta:");
    for (const auto& rec : inv2.value()) {
        show(rec, broker);
    }
    if (inv2.value().size() != 1 || inv2.value().front().value() != "order:cookie") {
        fail("inventory should see only the new cookie order");
    }
    if (!inventory.commit()) {
        fail("inventory commit delta");
    }

    auto an2 = drain(analytics);
    if (!an2) {
        fail("analytics delta poll");
        return 1;
    }
    ok("analytics delta: " + std::to_string(an2.value().size()) + " record(s)");
    if (an2.value().size() != 1) {
        fail("analytics should see the cookie too");
    }

    step("Protocol variant: unknown topic is an ErrorResponse, not a crash");
    const auto bad = broker.dispatch(ProduceRequest{"no-such-topic", -1, "k", "v"});
    if (!std::holds_alternative<ErrorResponse>(bad)) {
        fail("expected ErrorResponse");
    } else {
        ok(std::string("got ") + std::string(to_string(std::get<ErrorResponse>(bad).code)));
    }

    step("Restart broker — mmap log is on disk, fetch still works");
    broker.sync();
    const auto cookie_part = o4.value().partition;
    const auto cookie_off = o4.value().offset;
    {
        Broker restarted{dir};
        if (!restarted.create_topic("orders", 2)) {
            fail("reopen create_topic");
            return 1;
        }
        auto fetched = restarted.fetch("orders", cookie_part, cookie_off, 4096);
        if (!fetched || fetched.value().batch.empty()) {
            fail("persisted cookie missing");
            return 1;
        }
        const auto& rec = *fetched.value().batch.begin();
        ok("reopened log: " + std::string(rec.value()));
        if (rec.value() != "order:cookie") {
            fail("unexpected persisted value");
        }
        if (!fetched.value().batch.is_zero_copy(rec)) {
            fail("reopened fetch was not mmap zero-copy");
        } else {
            ok("reopened fetch still aliases mmap");
        }
        ok("note: group commits live in memory — a new process would re-read unless you persist offsets");
    }

    std::cout << "\nscenario " << (g_failed == 0 ? "ok" : "failed") << " (" << g_step
              << " steps, " << g_failed << " checks failed)\n";
    return g_failed == 0 ? 0 : 1;
}
