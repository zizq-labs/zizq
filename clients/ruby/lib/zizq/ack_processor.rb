# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

require "async"
require "async/barrier"

module Zizq
  # Dedicated background thread that processes ack/nack HTTP requests on
  # behalf of worker threads, decoupling job processing from network I/O.
  #
  # Workers push Ack/Nack items to a thread-safe queue. The processor runs
  # an async event loop that spawns an independent fiber per ack/nack
  # request, enabling true concurrent I/O over a single HTTP/2 connection.
  # Each fiber handles its own retries with exponential backoff.
  class AckProcessor
    # Immutable value object representing a successful job completion.
    Ack = Data.define(:job_id)

    # Immutable value object representing a job failure.
    Nack = Data.define(:job_id, :error, :error_type, :backtrace)

    # @rbs client: Client
    # @rbs capacity: Integer
    # @rbs logger: Logger
    # @rbs backoff: Backoff
    # @rbs return: void
    def initialize(client:, capacity:, logger:, backoff:)
      @client = client
      @logger = logger
      @backoff = backoff
      @queue = Thread::SizedQueue.new(capacity)
    end

    # Push an Ack or Nack to the processing queue.
    # Blocks if the queue is at capacity (backpressure).
    #
    # @rbs item: Ack | Nack
    # @rbs return: void
    def push(item)
      @queue.push(item)
    end

    # Start the background processor thread.
    def start #: () -> Thread
      @thread = Thread.new { run }
      @thread.name = "zizq-ack-processor"
      @thread
    end

    # Close the queue and wait for the processor to drain.
    #
    # @rbs timeout: Integer
    # @rbs return: void
    def stop(timeout: 10)
      @queue.close
      return unless @thread

      unless @thread.join(timeout)
        @logger.warn { "Ack processor did not drain in #{timeout}s, killing" }
        @thread.kill
      end
    end

    private

    def run #: () -> void
      Sync do
        barrier = Async::Barrier.new

        while (item = @queue.pop)
          barrier.async do
            process_item(item)
          end
        end

        barrier.wait
      end
    rescue => e
      @logger.error { "Ack processor crashed: #{e.class}: #{e.message}" }
      @logger.debug { e.backtrace&.join("\n") }
    end

    # Process a single ack/nack with inline retry logic.
    def process_item(item) #: (Ack | Nack) -> void
      backoff = @backoff.fresh

      begin
        case item
        when Ack
          @client.report_success(item.job_id)
        when Nack
          @client.report_failure(
            item.job_id,
            error: item.error,
            error_type: item.error_type,
            backtrace: item.backtrace
          )
        end
      rescue NotFoundError
        @logger.debug { "Ack/nack for #{item.job_id} returned 404 (already handled)" }
      rescue ClientError => e
        @logger.error { "Ack/nack for #{item.job_id} returned #{e.status} (dropping)" }
      rescue => e
        @logger.warn { "Retrying ack/nack for #{item.job_id} in #{backoff.duration}s: #{e.message}" }
        backoff.wait
        retry
      end
    end
  end
end
