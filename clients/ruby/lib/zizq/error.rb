# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

module Zizq
  # Base error class for all Zizq errors.
  class Error < StandardError; end

  # Network-level failure (connection refused, DNS, timeout etc).
  class ConnectionError < Error; end

  # HTTP error — the server returned a non-success status code.
  # Carries the status code and parsed body.
  class ResponseError < Error
    # The HTTP response status from the Zizq server.
    attr_reader :status #: Integer

    # The decoded body of the error response.
    attr_reader :body #: Hash[String, untyped]?

    # Create a new ResponseError with the given error message, response status
    # and decoded response body.
    def initialize(message, status:, body: nil) #: (String, status: Integer, ?body: Hash[String, untyped]?) -> void
      @status = status
      @body = body
      super(message)
    end
  end

  # 4xx client error.
  class ClientError < ResponseError; end

  # 404 specifically — job not found, etc.
  class NotFoundError < ClientError; end

  # 5xx server error.
  class ServerError < ResponseError; end

  # Streaming take-jobs connection interrupted.
  class StreamError < Error; end
end
