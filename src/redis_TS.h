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
  Slice combination_key;  // primary_key_clustering_id_ms;
  Slice value;
  int ttl;
} TSPairs;

namespace Redis {
class TS : public Database {
 public:
  explicit TS(Engine::Storage *storage, const std::string &ns)
      : Database(storage, ns) {}
  rocksdb::Status MAdd(const std::string primary_key,
                       const std::vector<TSPairs> &pairs);
  rocksdb::Status Add(const std::string primary_key, TSPairs &pair);
};
}  // namespace Redis
