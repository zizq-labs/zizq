# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

require "logger"

module Zizq
  # Global configuration for the Zizq client.
  #
  # The configuration stores only client-level concerns: server URL,
  # serialization format, and logger. Worker-specific settings (queues,
  # threads, etc.) are passed directly to the Worker.
  #
  # See: [`Zizq::configure]`.
  # See: [`Zizq::configuration]`.
  class Configuration
    # Base URL of the Zizq server (default: "http://localhost:7890").
    attr_accessor :url #: String

    # Choice of content-type encoding used in communication with the Zizq
    # server.
    #
    # One of: `:json`, `:msgpack` (default)
    attr_accessor :format #: Zizq::format

    # Logger instance to which to write log messages.
    attr_accessor :logger #: Logger

    def initialize #: () -> void
      @url = "http://localhost:7890"
      @format = :msgpack
      @logger = Logger.new($stdout, level: Logger::INFO)
    end

    # Validates that required configuration is present.
    def validate! #: () -> void
      raise ArgumentError, "Zizq.configure: url is required" if url.empty?

      unless %i[msgpack json].include?(format)
        raise ArgumentError, "Zizq.configure: format must be :msgpack or :json, got #{format.inspect}"
      end
    end
  end
end
