/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "redis_TS.h"

#include "db_util.h"

namespace Redis {
// TODO: add lock
rocksdb::Status TS::MAdd(const std::vector<TSAddSpec> &pairs) {
  rocksdb::WriteBatch batch;
  int64_t now;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  // LockGuard guard(storage_->GetLockManager(), primary_key);
  for (const auto &pair : pairs) {
    std::string ns_key;
    std::string bytes;
    Metadata metadata(kRedisString, false);
    int ttl = std::stoi(pair.ttl);
    if (ttl > 0) {
      metadata.expire = uint32_t(now) + ttl;
    } else {
      metadata.expire = 0;
    }
    metadata.Encode(&bytes);
    WriteBatchLogData log_data(kRedisTS);
    batch.PutLogData(log_data.Encode());
    bytes.append(pair.value);
    std::string combination_key = TSCombinKey::EncodeAddKey(pair);
    AppendNamespacePrefix(combination_key, &ns_key);
    batch.Put(ts_cf_handle_, ns_key, bytes);
  }
  auto s = storage_->Write(rocksdb::WriteOptions(), &batch);
  if (!s.ok()) return s;
  return rocksdb::Status::OK();
}

static rocksdb::Status FilterValue(const std::string &raw_value,
                                   std::string *value) {
  if (value) {
    value->clear();
  }
  Metadata metadata(kRedisNone, false);
  metadata.Decode(raw_value);
  if (metadata.Expired()) {
    return rocksdb::Status::Expired(kErrMsgKeyExpired);
  }
  *value = raw_value.substr(TS_HDR_SIZE);
  return rocksdb::Status::OK();
}

rocksdb::Status TS::Range(const TSRangSpec &rang_pair,
                          std::vector<TSFieldValue> *values) {
  long long from_timestamp = std::stoll(rang_pair.from_timestamp);
  long long to_timestamp = std::stoll(rang_pair.to_timestamp);
  int limit_num = std::stoi(rang_pair.limit_num);
  bool need_limit = "limit" == rang_pair.limit;
  bool order_desc = "desc" == rang_pair.order;

  if (need_limit && limit_num <= 0) {
    return rocksdb::Status::OK();
  }

  std::string ns_key, combin_key, value, ns;

  std::string prefix_key = TSCombinKey::MakePrefixKey(rang_pair);
  AppendNamespacePrefix(prefix_key, &ns_key);
  std::string prefix_key_p_f =
      ns_key.substr(0, ns_key.length() - TSCombinKey::TIMESTAMP_LEN);

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;

  auto iter = DBUtil::UniqueIterator(db_, read_options, ts_cf_handle_);
  for (order_desc ? iter->SeekForPrev(ns_key) : iter->Seek(ns_key);
       iter->Valid(); order_desc ? iter->Prev() : iter->Next()) {
    if (!iter->key().starts_with(prefix_key_p_f)) {
      break;
    }

    rocksdb::Status s = FilterValue(iter->value().ToString(), &value);
    if (!s.ok()) {
      continue;
    }

    ns.clear();
    combin_key.clear();
    ExtractNamespaceKey(iter->key(), &ns, &combin_key,
                        storage_->IsSlotIdEncoded());

    TSFieldValue tsfieldvalue = TSCombinKey::Decode(rang_pair, combin_key);

    if (!order_desc && to_timestamp > 0 &&
        std::stoll(tsfieldvalue.timestamp) > to_timestamp) {
      break;
    }

    if (order_desc && std::stoll(tsfieldvalue.timestamp) < from_timestamp) {
      break;
    }

    values->emplace_back(
        TSFieldValue{tsfieldvalue.field, tsfieldvalue.timestamp, value});

    if (need_limit && --limit_num <= 0) {
      break;
    }
  }
  return rocksdb::Status::OK();
}
}  // namespace Redis