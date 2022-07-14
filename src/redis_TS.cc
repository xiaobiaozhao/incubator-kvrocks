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
rocksdb::Status TS::MAdd(const std::string primary_key,
                         const std::vector<TSPair> &pairs) {
  rocksdb::WriteBatch batch;
  int64_t now;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  // LockGuard guard(storage_->GetLockManager(), primary_key);
  for (const auto &pair : pairs) {
    std::string ns_key;
    std::string bytes;
    Metadata metadata(kRedisString, false);
    if (pair.ttl > 0) {
      metadata.expire = uint32_t(now) + pair.ttl;
    } else {
      metadata.expire = 0;
    }
    metadata.Encode(&bytes);
    WriteBatchLogData log_data(kRedisTS);
    batch.PutLogData(log_data.Encode());
    bytes.append(pair.value.data(), pair.value.size());
    std::string combination_key = TSCombinKey::EncodeAddKey(pair);
    AppendNamespacePrefix(combination_key, &ns_key);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  auto s = storage_->Write(rocksdb::WriteOptions(), &batch);
  if (!s.ok()) return s;
  return rocksdb::Status::OK();
}

rocksdb::Status TS::Add(const std::string primary_key, TSPair &pair) {
  std::vector<TSPair> pairs{pair};
  return MAdd(primary_key, pairs);
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

rocksdb::Status TS::RangeAes(const TSPair &pair,
                             std::vector<TSFieldValue> *values) {
  std::string ns_key, combin_key, value, ns;
  u_int32_t limit_num = pair.limit_num;

  std::string prefix_key = TSCombinKey::MakePrefixKey(pair);
  AppendNamespacePrefix(prefix_key, &ns_key);

  auto iter =
      DBUtil::UniqueIterator(db_, rocksdb::ReadOptions(), metadata_cf_handle_);
  for (iter->Seek(ns_key); iter->Valid(); iter->Next()) {
    std::string x = iter->key().ToString();
    if (!iter->key().starts_with(ns_key.substr(0,
        ns_key.length() - TSCombinKey::TIMESTAMP_LEN))) {
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

    TSFieldValue tsfieldvalue = TSCombinKey::Decode(pair, combin_key);

    if (std::stoll(tsfieldvalue.timestamp) > pair.to_timestamp) {
      break;
    }

    values->emplace_back(TSFieldValue{tsfieldvalue.clustering_id,
                                      tsfieldvalue.timestamp, value});
    if (pair.limit && --limit_num <= 0) {
      break;
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status TS::Range(const TSPair &pair,
                          std::vector<TSFieldValue> *values) {
  // LatestSnapShot ss(db_);
  // rocksdb::ReadOptions read_options;
  // read_options.snapshot = ss.GetSnapShot();
  // read_options.fill_cache = false;
  if (pair.limit && pair.limit_num <= 0) {
    return rocksdb::Status::OK();
  }

  if (pair.aes) {
    return RangeAes(pair, values);
  } else {
    return rocksdb::Status::OK();
  }
}
}  // namespace Redis