# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

module Zizq
  module Resources
    # Paginated list of error records.
    # @rbs inherits Page[ErrorRecord]
    class ErrorPage < Page
      def items #: () -> Array[ErrorRecord]
        @items ||= (@data["errors"] || []).map { |e| ErrorRecord.new(client, e) }
      end

      alias errors items
    end
  end
end
