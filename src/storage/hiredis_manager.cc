#include "storage/hiredis_manager.h"

#include <sys/types.h>

#include "async.h"
#include "hiredis.h"
#include "lua.h"

void connectCallback(const redisAsyncContext *c, int status) {
  if (status != REDIS_OK) {
    printf("Error: %s\n", c->errstr);
    return;
  }
  printf("Connected...\n");
}

void disconnectCallback(const redisAsyncContext *c, int status) {
  if (status != REDIS_OK) {
    printf("Error: %s\n", c->errstr);
    return;
  }
  printf("Disconnected...\n");
}
void CommandCallback(redisAsyncContext *c, void *r, void *privdata) {
  auto lua = (lua_State *)privdata;
  lua_resume(lua, 0);
}

HiredisManager::HiredisManager(event_base *base, lua_State *lua) : base_(base), lua_(lua) {}

void HiredisManager::SetHost(const std::string &ip, uint16_t port) {
  REDIS_OPTIONS_SET_TCP(&options_, ip.c_str(), port);
}
void HiredisManager::SetTimeOut(uint32_t sec) {
  struct timeval tv = {0};
  tv.tv_sec = sec;
  options_.connect_timeout = &tv;

  c_ = redisAsyncConnectWithOptions(&options_);

  redisLibeventAttach(c_, base_);
  redisAsyncSetConnectCallback(c_, connectCallback);
  redisAsyncSetDisconnectCallback(c_, disconnectCallback);
}

void HiredisManager::ExecCommand(const std::string &cmd) {
  lua_yield(lua_, 0);
  redisAsyncCommand(c_, CommandCallback, lua_, cmd.c_str());
}
