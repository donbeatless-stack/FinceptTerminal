// gateway/tests/test_symbol_table.cpp
#include "fmdg/symbol_table.h"

#include <gtest/gtest.h>

#include <string>
#include <thread>
#include <vector>

using fmdg::SymbolId;
using fmdg::SymbolTable;

TEST(SymbolTable, InternIsDenseAndIdempotent) {
    SymbolTable t;
    EXPECT_EQ(t.intern("AAPL"), SymbolId{0});
    EXPECT_EQ(t.intern("MSFT"), SymbolId{1});
    EXPECT_EQ(t.intern("AAPL"), SymbolId{0});
    EXPECT_EQ(t.size(), 2u);
    EXPECT_EQ(t.name(1), "MSFT");
    EXPECT_EQ(t.name(2), "") << "unknown id";
    EXPECT_EQ(t.find("MSFT"), SymbolId{1});
    EXPECT_FALSE(t.find("NVDA")) << "find never adds";
    EXPECT_EQ(t.size(), 2u);
}

TEST(SymbolTable, CapacityCapsGrowthButKnownNamesStillResolve) {
    SymbolTable t(3);
    EXPECT_TRUE(t.intern("A"));
    EXPECT_TRUE(t.intern("B"));
    EXPECT_TRUE(t.intern("C"));
    EXPECT_FALSE(t.intern("D")) << "full";
    EXPECT_EQ(t.intern("B"), SymbolId{1}) << "existing names still intern when full";
    EXPECT_EQ(t.size(), 3u);
}

// Many threads interning overlapping names must agree on every id, and the ids
// must stay dense (no id handed out twice, none skipped).
TEST(SymbolTable, ConcurrentInternAgrees) {
    SymbolTable t;
    constexpr int kThreads = 8, kNames = 2000;
    std::vector<std::vector<SymbolId>> seen(kThreads, std::vector<SymbolId>(kNames));
    std::vector<std::thread> threads;
    for (int th = 0; th < kThreads; ++th)
        threads.emplace_back([&, th] {
            for (int i = 0; i < kNames; ++i) {
                const int k = (i * 7 + th * 131) % kNames; // each thread in its own order
                seen[th][k] = *t.intern("S" + std::to_string(k));
            }
        });
    for (auto& th : threads)
        th.join();
    EXPECT_EQ(t.size(), static_cast<std::size_t>(kNames));
    std::vector<int> used(kNames, 0);
    for (int k = 0; k < kNames; ++k) {
        for (int th = 1; th < kThreads; ++th)
            ASSERT_EQ(seen[th][k], seen[0][k]) << "name S" << k;
        ASSERT_LT(seen[0][k], static_cast<SymbolId>(kNames));
        ++used[seen[0][k]];
        EXPECT_EQ(t.name(seen[0][k]), "S" + std::to_string(k));
    }
    for (int id = 0; id < kNames; ++id)
        ASSERT_EQ(used[id], 1) << "id " << id;
}
