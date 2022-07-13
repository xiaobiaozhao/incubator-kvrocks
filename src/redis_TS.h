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

#pragma once

#include <string>
#include <vector>

#include "redis_db.h"
#include "redis_metadata.h"

typedef struct {
  std::string &primary_key;
  std::string &timestamp;  // ms
  std::string &clustering_id;
  std::string &value;
  long long ttl;
  long long fromTimestamp;
  long long toTimestamp;
  bool limit;          // true need limit
  uint32_t limit_num;  // limit num
  bool aes;            // true order aes, false desc
} TSPair;

typedef struct {
  std::string clustering_id;
  std::string timestamp;
  std::string value;
} TSFieldValue;

namespace Redis {
class TSCombinKey {
 public:
  static std::string EncodeAddKey(const TSPair &tspair) {
    // primary_key + cluster_id + timestamp(20B)
    // just for readful
    return tspair.primary_key + tspair.clustering_id +
           std::string(TIMESTAMP_LEN - tspair.timestamp.length(), '0') +
           tspair.timestamp;
  }

  static TSFieldValue Decode(const TSPair &tspair,
                             const std::string &combin_key) {
    return TSFieldValue{
        combin_key.substr(combin_key.find_first_not_of(
            '0', combin_key.length() - TIMESTAMP_LEN)),
        combin_key.substr(
            tspair.primary_key.length(),
            combin_key.length() - tspair.primary_key.length() - TIMESTAMP_LEN),
        ""};
  }

  static std::string MakePrefixKey(const TSPair &pair) {
    std::string prefix_key;
    prefix_key.append(pair.primary_key);
    if (pair.clustering_id.empty()) {
      return prefix_key;
    }

    if ('*' == pair.clustering_id.back()) {
      prefix_key.append(
          pair.clustering_id.substr(0, pair.clustering_id.length() - 1));
    } else {
      prefix_key.append(pair.clustering_id);
    }

    return prefix_key;
  }

  const static int TIMESTAMP_LEN = 20;

 private:
};
const int TS_HDR_SIZE = 5;
class TS : public Database {
 public:
  explicit TS(Engine::Storage *storage, const std::string &ns)
      : Database(storage, ns) {}
  rocksdb::Status MAdd(const std::string primary_key,
                       const std::vector<TSPair> &pairs);
  rocksdb::Status Add(const std::string primary_key, TSPair &pair);
  rocksdb::Status Range(TSPair &pair, std::vector<TSFieldValue> *values);
};
}  // namespace Redis
