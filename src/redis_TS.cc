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
    // uint32_t expire = 0;
    std::string bytes;
    Metadata metadata(kRedisString, false);
    metadata.expire = uint32_t(now) + pair.ttl;
    metadata.Encode(&bytes);
    WriteBatchLogData log_data(kRedisTS);
    batch.PutLogData(log_data.Encode());
    bytes.append(pair.value.data(), pair.value.size());
    AppendNamespacePrefix(pair.combination_key, &ns_key);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  auto s = storage_->Write(rocksdb::WriteOptions(), &batch);
  if (!s.ok()) return s;
  return rocksdb::Status::OK();
}

rocksdb::Status TS::Add(const std::string primary_key, TSPairs &pair) {
  std::vector<TSPairs> pairs{pair};
  return MAdd(primary_key, pairs);
}
}  // namespace Redis