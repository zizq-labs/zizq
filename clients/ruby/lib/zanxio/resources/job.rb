# Copyright (c) 2026 Chris Corbyn <chris@zanxio.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

module Zanxio
  module Resources
    # Typed wrapper around a job response hash.
    #
    # Exposes named accessor methods with Ruby-idiomatic types (fractional
    # seconds instead of milliseconds) and link methods that follow related
    # resources through the Client.
    class Job < Resource
      def id          = @data["id"]          #: () -> String
      def type        = @data["type"]        #: () -> String
      def queue       = @data["queue"]       #: () -> String
      def priority    = @data["priority"]    #: () -> Integer
      def status      = @data["status"]      #: () -> String
      def ready_at    = ms_to_seconds(@data["ready_at"])    #: () -> Float?
      def attempts    = @data["attempts"]    #: () -> Integer
      def payload     = @data["payload"]     #: () -> Hash[String, untyped]?
      def dequeued_at = ms_to_seconds(@data["dequeued_at"]) #: () -> Float?
      def failed_at   = ms_to_seconds(@data["failed_at"])   #: () -> Float?
      def retry_limit = @data["retry_limit"] #: () -> Integer?

      # Backoff configuration converted from the wire format (ms) to the
      # Ruby-idiomatic format (seconds), matching the Zanxio::backoff type.
      def backoff #: () -> Zanxio::backoff?
        raw = @data["backoff"]
        return nil unless raw

        {
          exponent: raw["exponent"].to_f,
          base: raw["base_ms"] / 1000.0,
          jitter: raw["jitter_ms"] / 1000.0
        }
      end

      # Fetch the error history for this job.
      #
      # @rbs from: String?
      # @rbs order: Zanxio::sort_direction?
      # @rbs limit: Integer?
      # @rbs return: ErrorPage
      def errors(from: nil, order: nil, limit: nil)
        @client.list_errors(id, from:, order:, limit:)
      end

      # Mark this job as successfully completed.
      def complete! #: () -> nil
        @client.report_success(id)
      end

      # Report this job as failed.
      #
      # @rbs error: String
      # @rbs error_type: String?
      # @rbs backtrace: String?
      # @rbs retry_at: Float?
      # @rbs kill: bool
      # @rbs return: Job
      def fail!(error:, error_type: nil, backtrace: nil, retry_at: nil, kill: false)
        @client.report_failure(id, error:, error_type:, backtrace:, retry_at:, kill:)
      end
    end
  end
end
