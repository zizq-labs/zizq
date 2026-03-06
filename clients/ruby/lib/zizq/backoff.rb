# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

module Zizq
  # Encapsulates exponential backoff state for retry loops.
  #
  # Each call to `wait` sleeps for the current duration and then advances
  # to the next interval. Call `reset` to return to the initial wait time
  # after a successful operation.
  class Backoff
    attr_reader :min_wait #: Float
    attr_reader :max_wait #: Float
    attr_reader :multiplier #: Float

    # @rbs min_wait: (Float | Integer)
    # @rbs max_wait: (Float | Integer)
    # @rbs multiplier: (Float | Integer)
    # @rbs return: void
    def initialize(min_wait:, max_wait:, multiplier:)
      @min_wait = min_wait.to_f
      @max_wait = max_wait.to_f
      @multiplier = multiplier.to_f
      @current = @min_wait #: Float
    end

    # Returns the current backoff duration without advancing.
    def duration #: () -> Float
      @current
    end

    # Sleeps for the current backoff duration, then advances to the next.
    def wait #: () -> void
      sleep @current
      @current = [@current * @multiplier, @max_wait].min
    end

    # Resets the backoff to the initial min_wait.
    def reset #: () -> void
      @current = @min_wait
    end

    # Returns a new Backoff with the same configuration but reset state.
    def fresh #: () -> Backoff
      self.class.new(min_wait: @min_wait, max_wait: @max_wait, multiplier: @multiplier)
    end
  end
end
