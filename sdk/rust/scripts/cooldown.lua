-- cooldown.lua: the upstream answered 429. Drain the global bucket and block
-- every service until the cooldown has passed.
-- KEYS[1] global bucket hash
-- ARGV[1] cooldown in milliseconds, ARGV[2] rate, ARGV[3] burst
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1e6
local ms = tonumber(ARGV[1])
if ms < 1 then ms = 1 end
local until_ts = now + ms / 1000
-- Never shorten a cooldown that is already in force.
local cur = redis.call('HGET', KEYS[1], 'cd_until')
if cur and tonumber(cur) > until_ts then until_ts = tonumber(cur) end
redis.call('HSET', KEYS[1], 'tokens', '0', 'ts', tostring(now), 'cd_until', tostring(until_ts))
redis.call('EXPIRE', KEYS[1], math.ceil(tonumber(ARGV[3]) / tonumber(ARGV[2])) + math.ceil(ms / 1000) + 60)
return ms
