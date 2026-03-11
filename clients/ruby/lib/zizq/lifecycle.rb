# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

module Zizq
  # Thread-safe state machine for coordinating worker shutdown.
  #
  # States:
  #   :running  → normal operation
  #   :draining → stop accepting work, finish in-progress jobs
  #   :stopped  → all work drained, safe to disconnect
  #
  # Transitions: running -> draining -> stopped (forward only).
  #
  # All transitions are signal-trap safe — they use only atomic symbol
  # assignment and Queue#close for wakeups.
  class Lifecycle
    # @rbs return: void
    def initialize
      @state = :running #: :running | :draining | :stopped
      @drain_latch = Thread::Queue.new
      @stop_latch = Thread::Queue.new
    end

    # Non-blocking, lock-free check.
    def running? #: () -> bool
      @state == :running
    end

    # Transition to :draining.
    def drain! #: () -> void
      return unless @state == :running

      @state = :draining
      @drain_latch.close rescue nil
    end

    # Transition to :stopped.
    def stop! #: () -> void
      return if @state == :stopped

      @state = :stopped
      @stop_latch.close rescue nil
    end

    # Block until the state is no longer :running.
    def wait_while_running #: () -> void
      @drain_latch.pop
    end

    # Block until the state is :stopped.
    def wait_until_stopped #: () -> void
      @stop_latch.pop
    end
  end
end
