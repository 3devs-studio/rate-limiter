# frozen_string_literal: true

# Fairshare: fair-share rate limiting straight from Redis.
#
# The decision logic is the Lua in lib/fairshare/scripts, identical to what
# the rate limiter service runs, so an app using this gem and an app behind
# the service share the same buckets and the same answers.
#
#   limiter = Fairshare::Limiter.new(Redis.new(url: ENV["REDIS_URL"]), "thirdparty")
#   d = limiter.acquire("my-app", wait: 4)
#   raise "rate limited, retry in #{d.retry_after_ms} ms" unless d.allowed
require "json"
require "redis"

module Fairshare
  CONFIG_SCHEMA = 1

  class Error < StandardError; end
  class UnknownService < Error; end
  class NoConfig < Error; end

  Share = Struct.new(:rate, :burst, :weight, keyword_init: true)
  Spec = Struct.new(:rate, :burst, :services, keyword_init: true)
  Decision = Struct.new(:allowed, :mode, :retry_after_ms, :waited_ms, :global_tokens, :service_tokens, keyword_init: true) do
    def allowed?
      allowed
    end
  end

  SCRIPTS = {
    acquire: File.read(File.join(__dir__, "fairshare", "scripts", "acquire.lua")),
    cooldown: File.read(File.join(__dir__, "fairshare", "scripts", "cooldown.lua")),
    sync: File.read(File.join(__dir__, "fairshare", "scripts", "sync.lua"))
  }.freeze

  def self.config_key(upstream)
    "rl:{#{upstream}}:config"
  end

  # Write an upstream's shares to Redis without running the service.
  def self.publish(redis, upstream, rate:, burst:, services:, by: "fairshare-ruby")
    doc = {
      schema: CONFIG_SCHEMA, upstream: upstream, rate: rate, burst: burst,
      services: services.transform_values { |s| { rate: s.rate, burst: s.burst, weight: s.weight }.compact },
      published_at: Time.now.utc.strftime("%Y-%m-%dT%H:%M:%SZ"), published_by: by
    }
    redis.set(config_key(upstream), JSON.generate(doc))
  end

  # Split rate and burst by weight, the way the service does.
  def self.shares_from_weights(rate, burst, weights)
    total = weights.values.sum.to_f
    weights.transform_values do |w|
      Share.new(rate: rate * w / total, burst: [1.0, burst * w / total].max, weight: w)
    end
  end

  class Limiter
    attr_reader :upstream

    def initialize(redis, upstream, config_ttl: 10)
      @redis = redis
      @upstream = upstream
      @config_ttl = config_ttl
      @spec = nil
      @loaded = 0.0
      @shas = {}
    end

    # The upstream's shares as published to Redis (cached).
    def spec
      return @spec if @spec && now - @loaded < @config_ttl

      raw = @redis.get(Fairshare.config_key(@upstream))
      raise NoConfig, "no published config for upstream: #{@upstream}" if raw.nil?

      doc = JSON.parse(raw)
      unless doc["schema"] == CONFIG_SCHEMA
        raise Error, "published config for #{@upstream} has schema #{doc['schema']}, expected #{CONFIG_SCHEMA}"
      end

      services = doc["services"].transform_values { |s| Share.new(rate: s["rate"].to_f, burst: s["burst"].to_f, weight: s["weight"]) }
      @spec = Spec.new(rate: doc["rate"].to_f, burst: doc["burst"].to_f, services: services)
      @loaded = now
      @spec
    rescue Redis::BaseError
      raise if @spec.nil?

      @spec # Redis hiccup: keep what we have
    end

    # Ask once; never waits.
    def try_acquire(service, cost = 1)
      s = spec
      me = s.services[service]
      raise UnknownService, "unknown service: #{service}" if me.nil?

      # Same KEYS/ARGV layout as the service: global, mine, then the others by name.
      keys = [global_key, service_key(service)]
      args = [cost, s.rate, s.burst, me.rate, me.burst]
      s.services.keys.sort.each do |name|
        next if name == service

        keys << service_key(name)
        args << s.services[name].rate << s.services[name].burst
      end
      allowed, retry_ms, mode, g, sv = run(:acquire, keys, args)
      Decision.new(allowed: allowed.to_i == 1, mode: mode, retry_after_ms: retry_ms.to_i, waited_ms: 0,
                   global_tokens: g.to_f, service_tokens: sv.to_f)
    end

    # Keep asking until a token is granted or +wait+ seconds have elapsed.
    def acquire(service, cost: 1, wait: 0)
      start = now
      d = try_acquire(service, cost)
      until d.allowed
        remaining_ms = wait * 1000 - (now - start) * 1000
        break if remaining_ms <= 0

        sleep([[d.retry_after_ms, 5].max, remaining_ms, 1000].min / 1000.0)
        d = try_acquire(service, cost)
      end
      d.waited_ms = ((now - start) * 1000).to_i
      d
    end

    # The upstream answered 429: every service backs off for +retry_after+ seconds.
    def report_rate_limited(retry_after)
      s = spec
      run(:cooldown, [global_key], [[1, (retry_after * 1000).round].max, s.rate, s.burst])
      nil
    end

    # Lower the global bucket to what the upstream says is left. Never raises it.
    def sync_remaining(remaining)
      s = spec
      run(:sync, [global_key], [remaining, s.rate, s.burst])
      nil
    end

    # Refill every bucket. For demos and tests.
    def reset
      @redis.del(global_key, *spec.services.keys.map { |n| service_key(n) })
      nil
    end

    private

    def global_key
      "rl:{#{@upstream}}:g"
    end

    def service_key(service)
      "rl:{#{@upstream}}:s:#{service}"
    end

    def now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    # EVALSHA with a one-time SCRIPT LOAD when Redis does not know the script yet.
    def run(name, keys, args)
      @shas[name] ||= @redis.script(:load, SCRIPTS[name])
      @redis.evalsha(@shas[name], keys: keys, argv: args.map(&:to_s))
    rescue Redis::CommandError => e
      raise unless e.message.include?("NOSCRIPT")

      @shas[name] = @redis.script(:load, SCRIPTS[name])
      @redis.evalsha(@shas[name], keys: keys, argv: args.map(&:to_s))
    end
  end
end

require "fairshare/rack"
