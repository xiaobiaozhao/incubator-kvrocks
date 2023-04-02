#include <adapters/libevent.h>
#include <sys/types.h>

#include <cstdint>

#include "lua.h"
#include "lua.hpp"
#include "storage/compact_filter.h"
#include "storage/lock_manager.h"

class HiredisManager {
 public:
  explicit HiredisManager(event_base *base, lua_State *lua);
  ~HiredisManager() = default;
  HiredisManager(const HiredisManager &) = delete;
  HiredisManager &operator=(const HiredisManager &) = delete;
  void SetHost(const std::string &ip, uint16_t port);
  void SetTimeOut(uint32_t);
  void ExecCommand(const std::string &cmd);

 private:
  struct event_base *base_ = nullptr;
  redisOptions options_ = {0};
  redisAsyncContext *c_ = nullptr;
  lua_State *lua_ = nullptr;
};