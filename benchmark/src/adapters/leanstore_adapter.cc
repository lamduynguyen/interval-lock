#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/tatp/schema.h"
#include "benchmark/tpcc/schema.h"
#include "benchmark/utils/test_utils.h"
#include "benchmark/ycsb/schema.h"
#include "leanstore/schema.h"

#include <span>

template <class RecordBase>
LeanStoreAdapter<RecordBase>::LeanStoreAdapter(leanstore::LeanStore &db)
    : relation_(static_cast<std::type_index>(typeid(RecordBase))), db_(&db) {
  db_->RegisterTable(relation_, RecordBase::TYPE_ID);  // TYPE_ID is a part of the LeanStore stupid catalog
  tree_ = db_->RetrieveIndex(relation_);
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::SetComparisonOperator(leanstore::ComparisonOperator cmp) {
  tree_->SetComparisonOperator(db_->RetrieveComparisonFunc(cmp));
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::ScanImpl(const std::optional<typename RecordBase::Key> &lo,
                                            const std::optional<typename RecordBase::Key> &hi,
                                            const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb,
                                            bool scan_ascending, bool lo_inclusive, bool hi_inclusive) {
  u8 lo_key[RecordBase::MaxFoldLength()];
  u8 hi_key[RecordBase::MaxFoldLength()];
  u16 lo_len = 0;
  u16 hi_len = 0;
  if (lo) { lo_len = RecordBase::FoldKey(lo_key, *lo); }
  if (hi) { hi_len = RecordBase::FoldKey(hi_key, *hi); }

  auto read_cb = [&](std::span<u8> key, std::span<u8> payload) -> bool {
    typename RecordBase::Key typed_key;
    RecordBase::UnfoldKey(key.data(), typed_key);
    return found_record_cb(typed_key, *reinterpret_cast<const RecordBase *>(payload.data()));
  };

  leanstore::ScanRange range{{lo_key, lo_len}, {hi_key, hi_len}, lo_inclusive, hi_inclusive};
  auto op_ret = scan_ascending ? tree_->ScanAscending(range, read_cb) : tree_->ScanDescending(range, read_cb);
  if (op_ret == leanstore::OpResult::ABORT_TX) { throw leanstore::ex::AbortTransaction(); }
}

template <class RecordBase>
auto LeanStoreAdapter<RecordBase>::LookUp(const typename RecordBase::Key &r_key,
                                          const typename Adapter<RecordBase>::AccessRecordFunc &fn) -> bool {
  u8 key[RecordBase::MaxFoldLength()];
  auto len = RecordBase::FoldKey(key, r_key);
  auto op_ret =
    tree_->LookUp({key, len}, [&](std::span<const u8> pl) { fn(*reinterpret_cast<const RecordBase *>(pl.data())); });
  if (op_ret == leanstore::OpResult::ABORT_TX) { throw leanstore::ex::AbortTransaction(); }
  return op_ret == leanstore::OpResult::OK;
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::Scan(const std::optional<typename RecordBase::Key> &lo,
                                        const std::optional<typename RecordBase::Key> &hi,
                                        const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb,
                                        bool lo_inclusive, bool hi_inclusive) {
  ScanImpl(lo, hi, found_record_cb, true, lo_inclusive, hi_inclusive);
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::ScanDesc(const typename RecordBase::Key &lo, const typename RecordBase::Key &hi,
                                            const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb,
                                            bool lo_inclusive, bool hi_inclusive) {
  ScanImpl(lo, hi, found_record_cb, false, lo_inclusive, hi_inclusive);
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::Insert(const typename RecordBase::Key &r_key, const RecordBase &record) {
  u8 key[RecordBase::MaxFoldLength()];
  auto len    = RecordBase::FoldKey(key, r_key);
  auto op_ret = tree_->Insert({key, len}, {reinterpret_cast<const u8 *>(&record), record.PayloadSize()});
  if (op_ret == leanstore::OpResult::ABORT_TX) { throw leanstore::ex::AbortTransaction(); }
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::InsertRawPayload(const typename RecordBase::Key &r_key, std::span<const u8> record) {
  Ensure(record.size() == reinterpret_cast<const RecordBase *>(record.data())->PayloadSize());
  u8 key[RecordBase::MaxFoldLength()];
  auto len    = RecordBase::FoldKey(key, r_key);
  auto op_ret = tree_->Insert({key, len}, record);
  if (op_ret == leanstore::OpResult::ABORT_TX) { throw leanstore::ex::AbortTransaction(); }
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::Update(const typename RecordBase::Key &r_key, const RecordBase &record) {
  u8 key[RecordBase::MaxFoldLength()];
  auto len    = RecordBase::FoldKey(key, r_key);
  auto op_ret = tree_->Update({key, len}, {reinterpret_cast<const u8 *>(&record), record.PayloadSize()}, {});
  if (op_ret == leanstore::OpResult::ABORT_TX) { throw leanstore::ex::AbortTransaction(); }
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::UpdateRawPayload(const typename RecordBase::Key &r_key, std::span<const u8> record,
                                                    const typename Adapter<RecordBase>::AccessRecordFunc &fn) {
  Ensure(record.size() == reinterpret_cast<const RecordBase *>(record.data())->PayloadSize());
  u8 key[RecordBase::MaxFoldLength()];
  auto len    = RecordBase::FoldKey(key, r_key);
  auto op_ret = tree_->Update({key, len}, record, [&](std::span<const u8> payload) {
    fn(*reinterpret_cast<const RecordBase *>(payload.data()));
  });
  if (op_ret == leanstore::OpResult::ABORT_TX) { throw leanstore::ex::AbortTransaction(); }
}

template <class RecordBase>
auto LeanStoreAdapter<RecordBase>::UpdateInPlace(const typename RecordBase::Key &r_key,
                                                 const typename Adapter<RecordBase>::ModifyRecordFunc &fn,
                                                 [[maybe_unused]] FixedSizeDelta *delta) -> bool {
  u8 key[RecordBase::MaxFoldLength()];
  auto len    = RecordBase::FoldKey(key, r_key);
  auto op_ret = tree_->UpdateInPlace(
    {key, len}, [&](std::span<u8> payload) { fn(*reinterpret_cast<RecordBase *>(payload.data())); }, delta);
  if (op_ret == leanstore::OpResult::ABORT_TX) { throw leanstore::ex::AbortTransaction(); }
  return op_ret == leanstore::OpResult::OK;
}

template <class RecordBase>
auto LeanStoreAdapter<RecordBase>::Erase(const typename RecordBase::Key &r_key) -> bool {
  u8 key[RecordBase::MaxFoldLength()];
  auto len    = RecordBase::FoldKey(key, r_key);
  auto op_ret = tree_->Remove({key, len});
  if (op_ret == leanstore::OpResult::ABORT_TX) { throw leanstore::ex::AbortTransaction(); }
  return op_ret == leanstore::OpResult::OK;
}

template <class RecordBase>
auto LeanStoreAdapter<RecordBase>::Count() -> u64 {
  return tree_->CountEntries();
}

template <class RecordBase>
auto LeanStoreAdapter<RecordBase>::RelationSize() -> float {
  return tree_->SizeInMB();
}

template <class RecordBase>
auto LeanStoreAdapter<RecordBase>::RegisterBlob(std::span<u8> blob_payload) -> std::span<const u8> {
  return db_->CreateNewBlob(blob_payload);
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::LoadBlob(u8 *blob_handler, const std::function<void(std::span<const u8>)> &read_cb,
                                            bool partial_load) {
  auto blob = reinterpret_cast<leanstore::BlobState *>(blob_handler);
  Ensure((leanstore::BlobState::MIN_MALLOC_SIZE <= blob->MallocSize()) &&
         (leanstore::BlobState::MAX_MALLOC_SIZE >= blob->MallocSize()));
  db_->LoadBlob(blob, read_cb, partial_load);
}

template <class RecordBase>
void LeanStoreAdapter<RecordBase>::RemoveBlob(u8 *blob_handler) {
  auto blob = reinterpret_cast<leanstore::BlobState *>(blob_handler);
  db_->RemoveBlob(blob);
}

// Misc
template struct LeanStoreAdapter<benchmark::RelationTest>;
template struct LeanStoreAdapter<benchmark::VariableSizeRelation>;

// For TPC-C
template struct LeanStoreAdapter<tpcc::WarehouseType>;
template struct LeanStoreAdapter<tpcc::DistrictType>;
template struct LeanStoreAdapter<tpcc::CustomerType>;
template struct LeanStoreAdapter<tpcc::CustomerWDCType>;
template struct LeanStoreAdapter<tpcc::HistoryType>;
template struct LeanStoreAdapter<tpcc::NewOrderType>;
template struct LeanStoreAdapter<tpcc::OrderType>;
template struct LeanStoreAdapter<tpcc::OrderWDCType>;
template struct LeanStoreAdapter<tpcc::OrderLineType>;
template struct LeanStoreAdapter<tpcc::ItemType>;
template struct LeanStoreAdapter<tpcc::StockType>;

// For TATP
template struct LeanStoreAdapter<tatp::SubscriberType>;
template struct LeanStoreAdapter<tatp::SubscriberNbrIndex>;
template struct LeanStoreAdapter<tatp::AccessInfoType>;
template struct LeanStoreAdapter<tatp::SpecialFacilityType>;
template struct LeanStoreAdapter<tatp::CallForwardingType>;

// For YCSB
template struct LeanStoreAdapter<ycsb::Relation<BytesPayload<120>, 0>>;
template struct LeanStoreAdapter<ycsb::BlobStateRelation>;
