# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

module Zizq
  module Resources
    # Base class for all typed response wrappers. Holds a reference to the
    # Client (for following links) and the raw response hash.
    class Resource
      # Returns the client instance that returned this resource.
      attr_reader :client #: Client

      # @rbs client: Zizq::Client
      # @rbs data: Hash[String, untyped]
      # @rbs return: void
      def initialize(client, data)
        @client = client
        @data = data
      end

      # Returns the underlying raw response hash.
      def to_h #: () -> Hash[String, untyped]
        @data
      end

      # Omit the client from inspect output to reduce noise.
      def inspect #: () -> String
        ivars = instance_variables
          .reject { |v| v == :@client }
          .map { |v| " #{v}=#{instance_variable_get(v).inspect}" }
          .join
        "#<#{self.class}#{ivars}>"
      end

      protected

      # Convert a millisecond timestamp to fractional seconds, nil-safe.
      def ms_to_seconds(value) #: (Integer?) -> Float?
        value&./(1000.0)
      end
    end
  end
end
