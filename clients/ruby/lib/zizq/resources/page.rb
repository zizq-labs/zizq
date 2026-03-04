# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

module Zizq
  module Resources
    # Base class for paginated list responses.
    #
    # Stores the raw response data and provides navigation helpers that
    # follow pagination links through the Client.
    #
    # @rbs generic T < Resource
    class Page < Resource
      # Wrapped resource objects for this page.
      def items #: () -> Array[T]
        raise NotImplementedError, "#{self.class.name}#items must be implemented"
      end

      # Returns true if there is a next page that can be fetched.
      def has_next? #: () -> bool
        !!@data.dig("pages", "next")
      end

      # Returns true if there is a previous page that can be fetched.
      def has_prev? #: () -> bool
        !!@data.dig("pages", "prev")
      end

      # Fetch the next page, or nil if there isn't one.
      def next_page #: () -> Page?
        path = @data.dig("pages", "next")
        return nil unless path

        wrap_page(client.get_path(path))
      end

      # Fetch the previous page, or nil if there isn't one.
      def prev_page #: () -> Page?
        path = @data.dig("pages", "prev")
        return nil unless path

        wrap_page(client.get_path(path))
      end

      private

      # Subclasses override to wrap the raw page data in the correct Page type.
      def wrap_page(data) #: (Hash[String, untyped]) -> Page
        self.class.new(client, data)
      end
    end
  end
end
