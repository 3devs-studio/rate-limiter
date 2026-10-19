# frozen_string_literal: true

require "json"

module Fairshare
  # Rack middleware: admit or refuse incoming requests on behalf of the
  # service that sent them. Rails:
  #
  #   # config/initializers/fairshare.rb
  #   LIMITER = Fairshare::Limiter.new(Redis.new(url: ENV["REDIS_URL"]), "thirdparty")
  #   Rails.application.config.middleware.use Fairshare::Rack, limiter: LIMITER,
  #     service: ->(env) { env["HTTP_X_SERVICE"] }, wait: 4
  class Rack
    DEFAULT_SERVICE = ->(env) { env["HTTP_X_SERVICE"] }

    def initialize(app, limiter:, service: DEFAULT_SERVICE, wait: 0, cost: 1, fail_open: true)
      @app = app
      @limiter = limiter
      @service = service
      @wait = wait
      @cost = cost
      @fail_open = fail_open
    end

    def call(env)
      service = @service.call(env)
      return @app.call(env) if service.nil? || service.empty?

      begin
        d = @limiter.acquire(service, cost: @cost, wait: @wait)
      rescue StandardError
        return [503, { "Content-Type" => "application/json" }, [JSON.generate(error: "rate limiter unavailable")]] unless @fail_open

        status, headers, body = @app.call(env)
        return [status, headers.merge("X-Fair-Share-Mode" => "unavailable"), body]
      end

      extra = { "X-Fair-Share-Service" => service, "X-Fair-Share-Mode" => d.mode, "X-Fair-Share-Wait-Ms" => d.waited_ms.to_s }
      unless d.allowed
        extra["Retry-After"] = [1, (d.retry_after_ms / 1000.0).ceil].max.to_s
        extra["Content-Type"] = "application/json"
        return [429, extra, [JSON.generate(error: "Too Many Requests", service: service, mode: d.mode, retryAfterMs: d.retry_after_ms)]]
      end

      status, headers, body = @app.call(env)
      [status, headers.merge(extra), body]
    end
  end
end
