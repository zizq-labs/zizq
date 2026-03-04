# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

module Zizq
  module Resources
    # Typed wrapper around a single error record from the job error history.
    class ErrorRecord < Resource
      def attempt     = @data["attempt"]     #: () -> Integer
      def message     = @data["message"]     #: () -> String
      def error_type  = @data["error_type"]  #: () -> String?
      def backtrace   = @data["backtrace"]   #: () -> String?
      def dequeued_at = ms_to_seconds(@data["dequeued_at"]) #: () -> Float
      def failed_at   = ms_to_seconds(@data["failed_at"])   #: () -> Float
    end
  end
end
