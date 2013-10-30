# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)

Gem::Specification.new do |s|
  s.name        = "fluent-plugin-hash-forward"
  s.version     = "0.0.1"
  s.authors     = ["Ryosuke IWANAGA", "Naotoshi SEO"]
  s.email       = ["riywo.jp@gmail.com", "sonots@gmail.com"]
  s.homepage    = "https://github.com/riywo/fluent-plugin-hash-forward"
  s.summary     = %q{Fluentd plugin to keep forwarding messsages of a specific tag pattern to a specific node}
  s.description = s.summary
  s.licenses    = ["MIT"]

  s.rubyforge_project = "fluent-plugin-hash-forward"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_runtime_dependency "fluentd"
  s.add_runtime_dependency "murmurhash3"
  s.add_development_dependency "rake"
  s.add_development_dependency "rspec"
  s.add_development_dependency "pry"
  s.add_development_dependency "pry-nav"
end
