# Copyright (c) 2026 Chris Corbyn <chris@zanxio.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

require "logger"

module Zanxio
  # Orchestrates fetching jobs from the server and dispatching them to
  # a pool of worker tasks for processing.
  #
  # Fiber support (`fiber_count > 1`) requires the `async` gem. When
  # `fiber_count == 1`, no async dependency is loaded.
  #
  # Total concurrency is calculated as `thread_count * fiber_count`.
  class Worker
    DEFAULT_THREADS = 5 #: Integer
    DEFAULT_FIBERS = 1 #: Integer
    DEFAULT_SHUTDOWN_TIMEOUT = 30 #: Integer
    DEFAULT_RECONNECT_INTERVAL = 1
    DEFAULT_MAX_RECONNECT_INTERVAL = 30
    DEFAULT_RECONNECT_EXPONENT = 2

    # Convenience class method to create and run a worker.
    def self.run(...) #: (**untyped) -> void
      new(...).run
    end

    # The total number of worker threads to run.
    #
    # For applications that are not threadsafe, this should be set to 1
    # (default: 5).
    attr_reader :thread_count #: Integer

    # The total number of fibers to run within each worker thread.
    #
    # For applications that cannot handle multi-fiber execution, this should be
    # set to 1. Any value greater than 1 requires loading 'async', which must
    # be made available by the application itself. Zanxio does not directly
    # depend on async (default: 1).
    attr_reader :fiber_count #: Integer

    # The set of queues from which to fetch jobs.
    #
    # An empty set (default) means all queues.
    attr_reader :queues #: Array[String]

    # The total number of jobs to allow to be sent from the server at once.
    #
    # This should be at least the number of worker threads * fibers (default).
    attr_reader :prefetch #: Integer

    # The maximum amount of time to wait for all workers to wrap up on shutdown.
    #
    # Once this timeout is reached, worker tasks are forcibly killed, which
    # will cause any in-flight jobs to be returned to the queue. No jobs are
    # lost (default: 30).
    attr_reader :shutdown_timeout #: Integer

    # The number of seconds to wait before reconnecting if a disconnect occurs.
    #
    # This time is multiplied by the `reconnect_exponent` value on each
    # attempt (default: 1).
    attr_reader :reconnect_interval #: Float

    # The maximum number of seconds to wait while attempting to reconnect.
    #
    # The wait time increases exponentially (by `reconnect_exponent`) up to
    # this limit (default: 30).
    attr_reader :max_reconnect_interval #: Float

    # The multiplier applied to the reconnect interval after each failed
    # attempt (default: 2).
    attr_reader :reconnect_exponent #: Float

    # Proc to derive a worker ID string for each thread and fiber.
    #
    # When not present, the Zanxio server assigns a random worker ID.
    attr_reader :worker_id_proc #: (^(Integer, Integer) -> String?)?

    # An instance of a Logger to be used for worker logging.
    attr_reader :logger #: Logger

    # @rbs queues: Array[String]
    # @rbs thread_count: Integer
    # @rbs fiber_count: Integer
    # @rbs prefetch: Integer?
    # @rbs shutdown_timeout: Integer
    # @rbs reconnect_interval: (Float | Integer)
    # @rbs max_reconnect_interval: (Float | Integer)
    # @rbs reconnect_exponent: (Float | Integer)
    # @rbs worker_id: (^(Integer, Integer) -> String?)?
    # @rbs logger: Logger?
    # @rbs return: void
    def initialize(
      queues: [],
      thread_count: DEFAULT_THREADS,
      fiber_count: DEFAULT_FIBERS,
      prefetch: nil,
      shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
      reconnect_interval: DEFAULT_RECONNECT_INTERVAL,
      max_reconnect_interval: DEFAULT_MAX_RECONNECT_INTERVAL,
      reconnect_exponent: DEFAULT_RECONNECT_EXPONENT,
      worker_id: nil,
      logger: nil
    )
      raise ArgumentError, "thread_count must be at least 1 (got #{thread_count})" if thread_count < 1
      raise ArgumentError, "fiber_count must be at least 1 (got #{fiber_count})" if fiber_count < 1

      @queues = queues
      @thread_count = thread_count
      @fiber_count = fiber_count
      @prefetch = prefetch || thread_count * fiber_count
      @shutdown_timeout = shutdown_timeout
      @reconnect_interval = reconnect_interval.to_f
      @max_reconnect_interval = max_reconnect_interval.to_f
      @reconnect_exponent = reconnect_exponent.to_f
      @worker_id_proc = worker_id
      @logger = logger || Zanxio.configuration.logger
      @shutdown = false
      @dispatch_queue = Thread::Queue.new
      @shutdown_latch = Thread::Queue.new
    end

    # Start the worker.
    #
    # Spawns the desired number of worker threads and fibers, distributes jobs
    # to those workers and then blocks until shutdown.
    def run #: () -> void
      install_signal_handlers

      logger.info { "Zanxio worker starting: #{thread_count} threads, #{fiber_count} fibers, prefetch=#{prefetch}" }
      logger.info { "Queues: #{queues.empty? ? '(all)' : queues.join(', ')}" }

      # Everything runs in the background initially.
      worker_threads = start_worker_threads
      producer_thread = start_producer_thread

      # Block until shutdown is signaled or the producer exits unexpectedly.
      # The latch is closed by the signal handler or the producer's ensure
      # block — pop returns nil in either case, waking the main thread.
      @shutdown_latch.pop
      logger.info { "Shutting down, waiting up to #{shutdown_timeout}s for workers to finish..." }

      # The producer has no work to drain — kill it immediately. It may
      # be blocked in a streaming HTTP read that neither Thread#raise nor
      # Thread#kill can interrupt (the exception is deferred until the
      # next GVL checkpoint, which won't happen until data arrives on the
      # socket). Give it a brief moment to exit cleanly, then force-kill.
      producer_thread.kill unless producer_thread.join(1)

      # Workers get the full shutdown timeout to drain remaining jobs.
      join_with_deadline(worker_threads)

      logger.info { "Zanxio worker stopped" }
    end

    private

    def install_signal_handlers #: () -> void
      %w[INT TERM].each do |signal|
        Signal.trap(signal) do
          next if @shutdown

          @shutdown = true
          @dispatch_queue.close
          @shutdown_latch.close
        end
      end
    end

    def start_producer_thread #: () -> Thread
      Thread.new do
        Thread.current.name = "zanxio-producer"
        current_interval = reconnect_interval

        until @shutdown
          begin
            client = Zanxio.client
            logger.info { "Connected to #{client.url}, streaming jobs..." }
            current_interval = reconnect_interval # reset on successful connect

            client.take_jobs(prefetch:, queues:) do |job_hash|
              begin
                @dispatch_queue.push(job_hash)
              rescue ClosedQueueError
                # Shutdown in progress — stop consuming from stream.
                # Exiting the block causes the HTTP connection to close,
                # and the server will requeue any unacknowledged jobs.
                break
              end
            end
          rescue Zanxio::ConnectionError, Zanxio::StreamError => e
            break if @shutdown

            logger.warn { "Stream disconnected: #{e.message}. Reconnecting in #{current_interval}s..." }
            sleep current_interval
            current_interval = [current_interval * reconnect_exponent, max_reconnect_interval].min
          rescue => e
            break if @shutdown

            logger.error { "Producer error: #{e.class}: #{e.message}" }
            logger.debug { e.backtrace&.join("\n") }
            sleep current_interval
            current_interval = [current_interval * reconnect_exponent, max_reconnect_interval].min
          end
        end

        # Ensure queue is closed so workers can drain and exit
        @dispatch_queue.close rescue nil
        logger.info { "Producer stopped" }
      ensure
        # Wake the main thread whether we exited due to shutdown or a crash.
        @shutdown_latch.close rescue nil
      end
    end

    def start_worker_threads #: () -> Array[Thread]
      (0...thread_count).map do |thread_idx|
        Thread.new(thread_idx) do |tidx|
          Thread.current.name = "zanxio-worker-#{tidx}"

          if fiber_count > 1
            run_fiber_workers(tidx)
          else
            run_loop(tidx, 0)
          end
        end
      end
    end

    # Internal worker run loop.
    #
    # Each worker thread or fiber continually pops jobs from the internal queue
    # and dispatches them to the correct job class until the queue is closed
    # and drained.
    def run_loop(thread_idx, fiber_idx) #: (Integer, Integer) -> void
      wid = resolve_worker_id(thread_idx, fiber_idx)
      client = Zanxio.client

      loop do
        job_hash = @dispatch_queue.pop
        # pop returns nil when queue is closed and empty
        break if job_hash.nil?

        dispatch(client, job_hash, wid)
      end
    end

    # Fiber-based worker loop. Requires the `async` gem.
    def run_fiber_workers(thread_idx) #: (Integer) -> void
      require "async"

      Async do |task|
        fiber_count.times do |fiber_idx|
          task.async do
            run_loop(thread_idx, fiber_idx)
          end
        end
      end
    end

    # Process a single job: resolve and validate the job class, instantiate,
    # set job metadata, call perform, then report success or failure.
    #
    # The outer rescue Exception catches anything the inner rescue misses,
    # including non-StandardError exceptions and errors from malformed job
    # hashes. This keeps the worker thread alive regardless of what happens
    # during dispatch.
    def dispatch(client, job, worker_id) #: (Client, Resources::Job, String?) -> void
      job_type = job.type
      job_id = job.id

      logger.debug { "Processing #{job_type} (#{job_id})" }

      # Resolve the job class from the type string and validate that it
      # includes Zanxio::Job. Object.const_get naturally triggers
      # Zeitwerk/autoload if configured.
      job_class = begin
        Object.const_get(job_type)
      rescue NameError => e
        logger.error { "Unknown job type '#{job_type}' (#{job_id}): #{e.message}" }
        safe_nack(client, job_id, e, worker_id: worker_id)
        return
      end

      unless job_class.is_a?(Class) && job_class.include?(Zanxio::Job)
        logger.error { "#{job_type} (#{job_id}) does not include Zanxio::Job" }
        safe_nack(
          client,
          job_id,
          RuntimeError.new("#{job_type} does not include Zanxio::Job"),
          worker_id:,
        )
        return
      end

      job_instance = job_class.new
      job_instance.set_zanxio_job(job)

      begin
        job_instance.perform(job.payload || {})
        safe_ack(client, job_id, worker_id: worker_id)
        logger.debug { "Completed #{job_type} (#{job_id})" }
      rescue Exception => e # Intentionally rescuing all Exceptions here
        logger.error { "#{job_type} (#{job_id}) failed: #{e.class}: #{e.message}" }
        safe_nack(client, job_id, e, worker_id: worker_id)
      end
    rescue Exception => e
      # Last resort in the case of something truly unexpected.
      logger.error { "Dispatch error (#{job_id || 'unknown'}): #{e.class}: #{e.message}" }
      safe_nack(client, job_id, e, worker_id: worker_id) if job_id
    end

    def safe_ack(client, job_id, worker_id: nil) #: (Client, String, ?worker_id: String?) -> void
      client.report_success(job_id)
    rescue => e
      logger.error { "Failed to ack #{job_id}: #{e.message}" }
    end

    def safe_nack(client, job_id, error, worker_id: nil) #: (Client, String, Exception, ?worker_id: String?) -> void
      client.report_failure(
        job_id,
        error: "#{error.class}: #{error.message}",
        error_type: error.class.name,
        backtrace: error.backtrace&.join("\n")
      )
    rescue => e
      logger.error { "Failed to nack #{job_id}: #{e.message}" }
    end

    def resolve_worker_id(thread_idx, fiber_idx) #: (Integer, Integer) -> String?
      worker_id_proc&.call(thread_idx, fiber_idx)
    end

    # Join all threads within the shutdown timeout. Any thread that hasn't
    # finished by the deadline is forcibly killed.
    #
    # Thread#join(timeout) returns nil when the timeout expires without the
    # thread finishing — it does NOT kill the thread. We must kill it
    # explicitly so we don't leave zombie threads running after shutdown.
    def join_with_deadline(threads) #: (Array[Thread]) -> void
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + shutdown_timeout

      threads.each do |t|
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        if remaining > 0 && t.join(remaining)
          next # Thread finished cleanly
        end

        logger.warn { "Shutdown timeout reached, killing thread #{t.name}" }
        t.kill
      end
    end
  end
end
