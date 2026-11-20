#include "common/util/small_vector.h"

#include "gtest/gtest.h"

#include <algorithm>
#include <array>

namespace leanstore::common {

namespace {
auto FillAscending(SmallVector<int> &vec, u32 n) -> void {
  for (u32 i = 0; i < n; ++i) { vec.push_back(static_cast<int>(i)); }
}
}  // namespace

TEST(TestSmallVector, SpillMovesTheWholeContentIntoOneContiguousBuffer) {
  SmallVector<int> vec;
  FillAscending(vec, 4);
  ASSERT_EQ(vec.size(), 4);
  const int *old_first = &vec[0];

  vec.push_back(4);  // first overflow relocates the whole content into the heap buffer
  ASSERT_EQ(vec.size(), 5);
  EXPECT_NE(&vec[0], old_first);
  for (u32 i = 0; i < vec.size(); ++i) { EXPECT_EQ(vec[i], static_cast<int>(i)); }
  EXPECT_EQ(vec.end() - vec.begin(), 5);  // one contiguous raw-pointer range

  vec.push_back(5);
  vec.push_back(6);
  vec.push_back(7);
  vec.push_back(8);  // growth past the first heap capacity relocates again, still contiguous
  ASSERT_EQ(vec.size(), 9);
  EXPECT_EQ(vec.end() - vec.begin(), 9);
  for (u32 i = 0; i < vec.size(); ++i) { EXPECT_EQ(vec[i], static_cast<int>(i)); }
}

TEST(TestSmallVector, IterationCoversInlineAndOverflowInOrder) {
  SmallVector<int> vec;
  FillAscending(vec, 10);
  int expected = 0;
  for (auto v : vec) { EXPECT_EQ(v, expected++); }

  auto found = std::find(vec.begin(), vec.end(), 6);  // lives in the overflow buffer
  ASSERT_NE(found, vec.end());
  EXPECT_EQ(*found, 6);
  EXPECT_EQ(std::find(vec.begin(), vec.end(), 10), vec.end());
}

TEST(TestSmallVector, EraseIfKeepsTheOrderOfSurvivors) {
  SmallVector<int> vec;
  FillAscending(vec, 10);  // inline 0..3, overflow 4..9
  vec.erase_if([](int v) { return v % 2 != 0; });
  EXPECT_EQ(vec.size(), 5);
  for (u32 i = 0; i < vec.size(); ++i) { EXPECT_EQ(vec[i], static_cast<int>(2 * i)); }
  vec.erase_if([](int v) { return v < 4; });
  EXPECT_EQ(vec.size(), 3);
  EXPECT_EQ(vec[0], 4);
  EXPECT_EQ(vec[2], 8);
}

TEST(TestSmallVector, TailOperationsAndClearReuse) {
  SmallVector<int> vec;
  FillAscending(vec, 6);
  EXPECT_EQ(vec.back(), 5);
  vec.pop_back();
  EXPECT_EQ(vec.size(), 5);
  vec.resize(2);  // shrink across the seam
  EXPECT_EQ(vec.size(), 2);
  EXPECT_EQ(vec[1], 1);
  vec.resize(6);  // value-init growth re-enters the overflow buffer
  EXPECT_EQ(vec.size(), 6);
  EXPECT_EQ(vec[5], 0);
  vec.clear();
  EXPECT_TRUE(vec.empty());
  vec.push_back(9);
  EXPECT_EQ(vec.size(), 1);
  EXPECT_EQ(vec.back(), 9);
}

}  // namespace leanstore::common

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
