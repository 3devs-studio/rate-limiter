# frozen_string_literal: true

# Runs sdk/conformance/cases.json against a real Redis (REDIS_ADDR, default localhost:6379).
require "minitest/autorun"
require "json"
require "redis"
require "fairshare"

SUITE = JSON.parse(File.read(File.expand_path("../../conformance/cases.json", __dir__)))
ADDR = ENV.fetch("REDIS_ADDR", "localhost:6379")
HOST, PORT = ADDR.split(":")
TOL = SUITE.fetch("tolerance", 0.1)

def redis_client
  Redis.new(host: HOST, port: PORT.to_i, connect_timeout: 1)
end

begin
  redis_client.ping
rescue StandardError => e
  warn "no Redis at #{ADDR} (#{e.message}); skipping conformance"
  exit 0
end

class ConformanceTest < Minitest::Test
  SUITE["cases"].each do |c|
    define_method("test_#{c['name'].tr('-', '_')}") do
      r = redis_client
      upstream = "conformance-ruby-#{c['name']}"
      services = c["spec"]["services"].transform_values { |s| Fairshare::Share.new(rate: s["rate"], burst: s["burst"]) }
      Fairshare.publish(r, upstream, rate: c["spec"]["rate"], burst: c["spec"]["burst"], services: services)
      lim = Fairshare::Limiter.new(r, upstream)
      lim.reset
      c["steps"].each_with_index do |step, i|
        where = "#{c['name']} step #{i}"
        case step["op"]
        when "acquire"
          e = step["expect"]
          d = lim.acquire(step["service"], cost: step.fetch("cost", 1), wait: step.fetch("wait_ms", 0) / 1000.0)
          assert_equal e["allowed"], d.allowed, "#{where}: #{d}"
          assert_equal e["mode"], d.mode, where if e.key?("mode")
          assert_in_delta e["service_tokens"], d.service_tokens, TOL, where if e.key?("service_tokens")
          assert_in_delta e["global_tokens"], d.global_tokens, TOL, where if e.key?("global_tokens")
          assert_operator d.waited_ms, :>=, e["min_waited_ms"], where if e.key?("min_waited_ms")
          assert_operator d.retry_after_ms, :>=, e["min_retry_after_ms"], where if e.key?("min_retry_after_ms")
          assert_operator d.retry_after_ms, :<=, e["max_retry_after_ms"], where if e.key?("max_retry_after_ms")
        when "drain"
          count = 0
          last = ""
          while count < step.fetch("max", 1000)
            d = lim.try_acquire(step["service"], 1)
            last = d.mode
            break unless d.allowed

            count += 1
          end
          assert_equal step["expect_count"], count, where
          assert_equal step["expect_last_mode"], last, where if step.key?("expect_last_mode")
        when "acquire_error"
          assert_raises(Fairshare::UnknownService) { lim.try_acquire(step["service"], 1) }
        when "cooldown" then lim.report_rate_limited(step["ms"] / 1000.0)
        when "sync" then lim.sync_remaining(step["remaining"])
        when "sleep" then sleep(step["ms"] / 1000.0)
        when "reset" then lim.reset
        else raise "unknown op #{step['op']}"
        end
      end
    end
  end

  def test_rack_middleware
    r = redis_client
    Fairshare.publish(r, "conformance-rack", rate: 1, burst: 2, services: { "web" => Fairshare::Share.new(rate: 1, burst: 2) })
    lim = Fairshare::Limiter.new(r, "conformance-rack")
    lim.reset
    app = Fairshare::Rack.new(->(_env) { [200, { "Content-Type" => "text/plain" }, ["ok"]] }, limiter: lim)
    codes = 3.times.map { app.call("HTTP_X_SERVICE" => "web")[0] }
    assert_equal [200, 200, 429], codes
    status, headers, = app.call("HTTP_X_SERVICE" => "web")
    assert_equal 429, status
    assert_equal "global", headers["X-Fair-Share-Mode"]
    assert headers["Retry-After"]
    assert_equal 200, app.call({})[0] # no service header: unmanaged
  end
end
