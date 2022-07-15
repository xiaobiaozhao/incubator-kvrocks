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
  std::string &field;
  std::string &value;
  std::string &ttl;
} TSAddSpec;

typedef struct {
  std::string &primary_key;
  std::string &field;
  std::string &from_timestamp;
  std::string &to_timestamp;
  std::string &limit;      // true need limit
  std::string &limit_num;  // limit num
  std::string &order;      // true order aes, false desc
} TSRangSpec;

typedef struct {
  std::string field;
  std::string timestamp;
  std::string value;
} TSFieldValue;

namespace Redis {
class TSCombinKey {
 public:
  static std::string MakeTSTimestamp(const std::string &timestamp) {
    return std::string(TIMESTAMP_LEN - timestamp.length(), '0') + timestamp;
  }

  static std::string EncodeAddKey(const TSAddSpec &tspair) {
    // primary_key + cluster_id + timestamp(20B)
    // just for readful
    return tspair.primary_key + tspair.field +
           MakeTSTimestamp(tspair.timestamp);
  }

  static TSFieldValue Decode(const TSRangSpec &ts_rang_pair,
                             const std::string &combin_key) {
    return TSFieldValue{
        combin_key.substr(ts_rang_pair.primary_key.length(),
                          combin_key.length() -
                              ts_rang_pair.primary_key.length() -
                              TIMESTAMP_LEN),
        combin_key.substr(combin_key.find_first_not_of(
            '0', combin_key.length() - TIMESTAMP_LEN)),
        ""};
  }

  static std::string MakePrefixKey(const TSRangSpec &rang_pair) {
    std::string prefix_key;
    prefix_key.append(rang_pair.primary_key);
    prefix_key.append(rang_pair.field);
    if ("desc" == rang_pair.order) {
      if (std::stoll(rang_pair.to_timestamp) > 0) {
        prefix_key.append(MakeTSTimestamp(rang_pair.to_timestamp));
      } else {
        prefix_key.append(std::string(TIMESTAMP_LEN, '9'));
      }
    } else {
      prefix_key.append(MakeTSTimestamp(rang_pair.from_timestamp));
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
  rocksdb::Status MAdd(const std::vector<TSAddSpec> &pairs);
  rocksdb::Status Range(const TSRangSpec &rang_pair,
                        std::vector<TSFieldValue> *values);
};
}  // namespace Redis
