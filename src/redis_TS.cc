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
                         const std::vector<TSPairs> &pairs) {
  rocksdb::WriteBatch batch;
  int64_t now;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  LockGuard guard(storage_->GetLockManager(), primary_key);
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
    std::string combination_key =
        pair.primary_key + "~" + pair.clustering_id + "~" + pair.timestamp;
    AppendNamespacePrefix(combination_key, &ns_key);
    batch.Put(ns_key, bytes);
  }
  auto s = storage_->Write(rocksdb::WriteOptions(), &batch);
  if (!s.ok()) return s;
  return rocksdb::Status::OK();
}

rocksdb::Status TS::Add(const std::string primary_key, TSPairs &pair) {
  std::vector<TSPairs> pairs{pair};
  return MAdd(primary_key, pairs);
}

rocksdb::Status TS::Range(TSPairs &pair, std::vector<TSFieldValue> *values) {
  std::string ns_key;
  std::string prefix_key = "";
  prefix_key += pair.primary_key;

  AppendNamespacePrefix(prefix_key, &ns_key);

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = DBUtil::UniqueIterator(db_, rocksdb::ReadOptions());
  for (iter->Seek(ns_key); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(ns_key)) {
      break;
    }
    std::string ns;
    std::string user_key;
    ExtractNamespaceKey(iter->key(), &ns, &user_key,
                        storage_->IsSlotIdEncoded());
    values->emplace_back(TSFieldValue{
        user_key, "0", iter->value().ToString().substr(TS_HDR_SIZE)});
  }
  return rocksdb::Status::OK();
}
}  // namespace Redis