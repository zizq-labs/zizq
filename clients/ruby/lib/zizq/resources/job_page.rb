# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

module Zizq
  module Resources
    # Paginated list of jobs.
    # @rbs inherits Page[Job]
    class JobPage < Page
      def items #: () -> Array[Job]
        @items ||= (@data["jobs"] || []).map { |j| Job.new(client, j) }
      end

      alias jobs items
    end
  end
end
