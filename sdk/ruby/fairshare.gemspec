Gem::Specification.new do |s|
  s.name        = "fairshare"
  s.version     = "0.1.0"
  s.summary     = "Fair-share rate limiting client: talks to Redis directly and runs the same Lua scripts as the rate limiter service."
  s.authors     = ["3devs-studio"]
  s.license     = "MIT"
  s.files       = Dir["lib/**/*"] + ["README.md"]
  s.required_ruby_version = ">= 2.6"
  s.add_dependency "redis", ">= 4.8", "< 6"
end
