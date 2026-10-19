-- sync.lua: the upstream reported how many requests it will still accept.
-- Never trust ourselves to have more than that.
-- KEYS[1] global bucket hash
-- ARGV[1] remaining, ARGV[2] rate, ARGV[3] burst
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1e6
local remaining, rate, burst = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local v = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local g = burst
if v[1] then
  local elapsed = now - tonumber(v[2])
  if elapsed < 0 then elapsed = 0 end
  g = math.min(burst, tonumber(v[1]) + elapsed * rate)
end
if remaining < g then
  redis.call('HSET', KEYS[1], 'tokens', tostring(math.max(remaining, 0)), 'ts', tostring(now))
  redis.call('EXPIRE', KEYS[1], math.ceil(burst / rate) + 60)
  return tostring(remaining)
end
return tostring(g)
