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
    Nack = Data.define(:job_id, :message, :error_type, :backtrace)

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
          # Put the item into a batch.
          batch = [item]

          # Drain any additional ready items into the batch.
          loop do
            batch << @queue.pop(true) # non-blocking
          rescue ThreadError
            break
          end

          # Partition: acks go bulk, nacks go individually.
          acks, nacks = batch.partition { |i| i.is_a?(Ack) }

          unless acks.empty?
            barrier.async { process_ack_batch(acks) }
          end

          nacks.each do |nack|
            barrier.async { process_nack(nack) }
          end
        end

        barrier.wait
      end
    rescue => e
      @logger.error { "Ack processor crashed: #{e.class}: #{e.message}" }
      @logger.debug { e.backtrace&.join("\n") }
    end

    def process_ack_batch(acks) #: (Array[Ack]) -> void
      backoff = @backoff.fresh
      ids = acks.map(&:job_id)
      begin
        @client.report_success_bulk(ids)
      rescue ClientError => e
        @logger.warn { "Bulk ack (#{ids.size} jobs) returned #{e.status} (dropping: #{e.message})" }
      rescue => e
        @logger.warn { "Retrying bulk ack (#{ids.size} jobs) in #{backoff.duration}s: #{e.message}" }
        backoff.wait
        retry
      end
    end

    def process_nack(nack) #: (Nack) -> void
      backoff = @backoff.fresh
      begin
        @client.report_failure(
          nack.job_id,
          message: nack.message,
          error_type: nack.error_type,
          backtrace: nack.backtrace
        )
      rescue NotFoundError
        @logger.debug { "Nack for #{nack.job_id} returned 404 (already handled)" }
      rescue ClientError => e
        @logger.error { "Nack for #{nack.job_id} returned #{e.status} (dropping)" }
      rescue => e
        @logger.warn { "Retrying nack for #{nack.job_id} in #{backoff.duration}s: #{e.message}" }
        backoff.wait
        retry
      end
    end
  end
end
