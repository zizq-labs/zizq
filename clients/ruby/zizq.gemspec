# frozen_string_literal: true

require_relative "lib/zizq/version"

Gem::Specification.new do |spec|
  spec.name = "zizq"
  spec.version = Zizq::VERSION
  spec.authors = ["Chris Corbyn <chris@zizq.io>"]
  spec.license = "MIT"

  spec.summary = "The official Ruby client for the Zizq job queue"

  spec.description = "This is the Ruby client for the Zizq persistent job queue server.\n\n" \
                     "[Zizq](https://zizq.io/) is a lightweight, language agnostic job queue " \
                     "server enabling the enqueueing and processing of asynchronous, remote " \
                     "background jobs even in environments where multiple programming languages are " \
                     "used.\n\n" \
                     "Zizq is designed to be delightfully easy to set up and use, with a single " \
                     "self-contained binary providing performance and durability out of the box. " \
                     "No separate external storage dependencies to configure.\n\n" \
                     "Supports durably enqueuing jobs with both FIFO and priority-based strategies, " \
                     "streaming jobs to multi-threaded/multi-fiber workers, automatic backoff and retry " \
                     "handling, along with powerful queue visibility capabilities and a number of other " \
                     "essential features most projects eventually need and wish they had."

  spec.homepage = "https://github.com/d11wtq/zizq"
  spec.required_ruby_version = ">= 3.2"

  spec.files = Dir["lib/**/*.rb", "bin/**/*", "LICENSE"]
  spec.executables = ["zizq-worker"]
  spec.require_paths = ["lib"]

  spec.add_dependency "async-http", "~> 0.82"
  spec.add_dependency "msgpack", "~> 1.7"
end
