# frozen_string_literal: true

require_relative "lib/zanxio/version"

Gem::Specification.new do |spec|
  spec.name = "zanxio"
  spec.version = Zanxio::VERSION
  spec.authors = ["Chris Corbyn <chris@zanxio.io>"]
  spec.license = "MIT"

  spec.summary = "The official Ruby client for the Zanxio job queue"

  spec.description = "This is the Ruby client for the Zanxio persistent job queue server.\n\n" \
                     "[Zanxio](https://zanxio.io/) is a lightweight, language agnostic job queue " \
                     "server enabling the enqueueing and processing of asynchronous, remote " \
                     "background jobs even in environments where multiple programming languages are " \
                     "used.\n\n" \
                     "Zanxio is designed to be delightfully easy to set up and use, with a single " \
                     "self-contained binary providing performance and durability out of the box. " \
                     "No separate external storage dependencies to configure.\n\n" \
                     "Supports durably enqueuing jobs with both FIFO and priority-based strategies, " \
                     "streaming jobs to multi-threaded/multi-fiber workers, automatic backoff and retry " \
                     "handling, along with powerful queue visibility capabilities and a number of other " \
                     "essential features most projects eventually need and wish they had."

  spec.homepage = "https://github.com/d11wtq/zanxio"
  spec.required_ruby_version = ">= 3.1"

  spec.files = Dir["lib/**/*.rb", "bin/**/*", "LICENSE"]
  spec.executables = ["zanxio"]
  spec.require_paths = ["lib"]

  spec.add_dependency "httpx", "~> 1.4"
  spec.add_dependency "msgpack", "~> 1.7"
end
