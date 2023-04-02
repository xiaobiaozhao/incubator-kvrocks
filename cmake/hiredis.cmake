# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

include_guard()

include(cmake/utils.cmake)

FetchContent_DeclareGitHubWithMirror(hiredis
  redis/hiredis v1.1.0
  MD5=fee8ff6f43c155fcb0efd6b13835564b
)

if(NOT hiredis_POPULATED)
  FetchContent_Populate(hiredis)
  add_subdirectory(${hiredis_SOURCE_DIR} ${hiredis_BINARY_DIR})
  message(WARNING ${hiredis_SOURCE_DIR})
endif()

FetchContent_MakeAvailableWithArgs(hiredis)

add_library(hiredis_with_headers INTERFACE)
target_include_directories(hiredis_with_headers INTERFACE ${hiredis_SOURCE_DIR})
target_link_libraries(hiredis_with_headers INTERFACE hiredis::hiredis_static)