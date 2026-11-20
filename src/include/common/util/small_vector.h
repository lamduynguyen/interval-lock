#pragma once

#include "common/typedefs.h"

#include <cstddef>
#include <new>
#include <utility>

namespace leanstore::common {

template <class T, u32 INLINE_CAP = 4>
class SmallVector {
 public:
  SmallVector() = default;

  ~SmallVector() {
    Destroy(begin_, finish_);
    FreeStorage();
  }

  SmallVector(const SmallVector &)                     = delete;
  auto operator=(const SmallVector &) -> SmallVector & = delete;

  auto size() const -> u32 { return static_cast<u32>(finish_ - begin_); }

  auto empty() const -> bool { return begin_ == finish_; }

  auto data() -> T * { return begin_; }

  auto data() const -> const T * { return begin_; }

  auto begin() -> T * { return begin_; }

  auto end() -> T * { return finish_; }

  auto begin() const -> const T * { return begin_; }

  auto end() const -> const T * { return finish_; }

  auto operator[](u32 idx) -> T & { return begin_[idx]; }

  auto operator[](u32 idx) const -> const T & { return begin_[idx]; }

  auto back() -> T & { return finish_[-1]; }

  auto back() const -> const T & { return finish_[-1]; }

  auto push_back(const T &value) -> void {
    if (finish_ == cap_end_) { Grow(); }
    new (finish_) T(value);
    ++finish_;
  }

  auto pop_back() -> void { (--finish_)->~T(); }

  auto Init() -> void {
    Destroy(begin_, finish_);
    FreeStorage();
    begin_   = InlineBase();
    finish_  = begin_;
    cap_end_ = begin_ + INLINE_CAP;
  }

  auto clear() -> void {
    Destroy(begin_, finish_);
    finish_ = begin_;
  }

  auto resize(u32 n) -> void {
    const u32 cur = size();
    if (n < cur) {
      Destroy(begin_ + n, finish_);
      finish_ = begin_ + n;
      return;
    }
    for (u32 i = cur; i < n; ++i) { push_back(T()); }
  }

  template <class Pred>
  auto erase_if(Pred pred) -> u32 {
    const u32 n = size();
    u32 kept    = 0;
    for (u32 i = 0; i < n; ++i) {
      if (!pred(begin_[i])) {
        if (kept != i) { begin_[kept] = std::move(begin_[i]); }
        ++kept;
      }
    }
    Destroy(begin_ + kept, finish_);
    finish_ = begin_ + kept;
    return n - kept;
  }

 private:
  auto InlineBase() -> T * { return std::launder(reinterpret_cast<T *>(inline_)); }

  auto InlineBase() const -> const T * { return std::launder(reinterpret_cast<const T *>(inline_)); }

  auto Grow() -> void {
    const u32 cur_size = size();
    const u32 cur_cap  = static_cast<u32>(cap_end_ - begin_);
    u32 new_cap        = (cur_cap == 0 ? INLINE_CAP : cur_cap) * 2;
    while (new_cap < cur_size + 1) { new_cap *= 2; }
    auto *fresh = static_cast<T *>(::operator new(sizeof(T) * new_cap, std::align_val_t{alignof(T)}));
    DestroyAndMove(fresh);
    FreeStorage();
    begin_   = fresh;
    finish_  = fresh + cur_size;
    cap_end_ = fresh + new_cap;
  }

  auto DestroyAndMove(T *dst) -> void {
    for (T *p = begin_; p != finish_; ++p) {
      new (dst++) T(std::move(*p));
      p->~T();
    }
  }

  auto FreeStorage() -> void {
    if (begin_ != InlineBase()) { ::operator delete(begin_, std::align_val_t{alignof(T)}); }
  }

  auto Destroy(T *from, T *to) -> void {
    for (; from != to; ++from) { from->~T(); }
  }

  T *begin_{InlineBase()};
  T *finish_{begin_};
  T *cap_end_{begin_ + INLINE_CAP};
  alignas(T) std::byte inline_[sizeof(T) * INLINE_CAP];
};

}  // namespace leanstore::common
