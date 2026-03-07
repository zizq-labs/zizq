# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

require "logger"

module Zizq
  # Top-level worker process which orchestrates fetching jobs from the server
  # and dispatching them to a pool of worker tasks for processing.
  #
  # Fiber support (when `fiber_count > 1`) creates an Async context. When
  # `fiber_count == 1`, no Async context is created.
  #
  # Total concurrency is calculated as `thread_count * fiber_count`.
  class Worker
    DEFAULT_THREADS = 5 #: Integer
    DEFAULT_FIBERS = 1 #: Integer
    DEFAULT_SHUTDOWN_TIMEOUT = 30 #: Integer
    DEFAULT_RETRY_MIN_WAIT = 1
    DEFAULT_RETRY_MAX_WAIT = 30
    DEFAULT_RETRY_MULTIPLIER = 2

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
    # set to 1. Any value greater than 1 runs workers inside an Async context
    # (default: 1).
    attr_reader :fiber_count #: Integer

    # The set of queues from which to fetch jobs.
    #
    # An empty set (default) means all queues.
    attr_reader :queues #: Array[String]

    # The total number of jobs to allow to be sent from the server at once.
    #
    # Defaults to 2x the total concurrency (threads * fibers) to keep the
    # pipeline full while ack round-trips are in flight.
    attr_reader :prefetch #: Integer

    # The maximum amount of time to wait for all workers to wrap up on shutdown.
    #
    # Once this timeout is reached, worker tasks are forcibly killed, which
    # will cause any in-flight jobs to be returned to the queue. No jobs are
    # lost (default: 30).
    attr_reader :shutdown_timeout #: Integer

    # Backoff configuration used for reconnects and ack/nack retries.
    attr_reader :backoff #: Backoff

    # Proc to derive a worker ID string for each thread and fiber.
    #
    # When not present, the Zizq server assigns a random worker ID.
    attr_reader :worker_id_proc #: (^(Integer, Integer) -> String?)?

    # An instance of a Logger to be used for worker logging.
    attr_reader :logger #: Logger

    # @rbs queues: Array[String]
    # @rbs thread_count: Integer
    # @rbs fiber_count: Integer
    # @rbs prefetch: Integer?
    # @rbs shutdown_timeout: Integer
    # @rbs retry_min_wait: (Float | Integer)
    # @rbs retry_max_wait: (Float | Integer)
    # @rbs retry_multiplier: (Float | Integer)
    # @rbs worker_id: (^(Integer, Integer) -> String?)?
    # @rbs logger: Logger?
    # @rbs return: void
    def initialize(
      queues: [],
      thread_count: DEFAULT_THREADS,
      fiber_count: DEFAULT_FIBERS,
      prefetch: nil,
      shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
      retry_min_wait: DEFAULT_RETRY_MIN_WAIT,
      retry_max_wait: DEFAULT_RETRY_MAX_WAIT,
      retry_multiplier: DEFAULT_RETRY_MULTIPLIER,
      worker_id: nil,
      logger: nil
    )
      raise ArgumentError, "thread_count must be at least 1 (got #{thread_count})" if thread_count < 1
      raise ArgumentError, "fiber_count must be at least 1 (got #{fiber_count})" if fiber_count < 1

      @queues = queues
      @thread_count = thread_count
      @fiber_count = fiber_count
      @prefetch = prefetch || thread_count * fiber_count * 2
      @shutdown_timeout = shutdown_timeout
      @backoff = Backoff.new(
        min_wait:   retry_min_wait,
        max_wait:   retry_max_wait,
        multiplier: retry_multiplier
      )
      @worker_id_proc = worker_id
      @logger = logger || Zizq.configuration.logger
      @shutdown = false
      @dispatch_queue = Thread::Queue.new
      @shutdown_latch = Thread::Queue.new

      Zizq.configuration.validate!
      @ack_processor = AckProcessor.new(
        client:   Zizq.client,
        capacity: @prefetch * 2,
        logger:   @logger,
        backoff:  @backoff
      )
    end

    # Signal the worker to shut down gracefully.
    #
    # Closes the dispatch queue and unblocks the main run loop so it can
    # proceed through its shutdown sequence (drain workers, stop ack
    # processor). Safe to call from any thread.
    def shutdown #: () -> void
      return if @shutdown

      @shutdown = true
      @dispatch_queue.close rescue nil
      @shutdown_latch.close rescue nil
    end

    # Start the worker.
    #
    # Spawns the desired number of worker threads and fibers, distributes jobs
    # to those workers and then blocks until shutdown.
    def run #: () -> void
      install_signal_handlers

      logger.info { "Zizq worker starting: #{thread_count} threads, #{fiber_count} fibers, prefetch=#{prefetch}" }
      logger.info { "Queues: #{queues.empty? ? '(all)' : queues.join(', ')}" }

      # Everything runs in the background initially.
      @ack_processor.start
      worker_threads = start_worker_threads
      producer_thread = start_producer_thread

      # Block until shutdown is signaled or the producer exits unexpectedly.
      # The latch is closed by the signal handler or the producer's ensure
      # block — pop returns nil in either case, waking the main thread.
      @shutdown_latch.pop
      logger.info { "Shutting down. Waiting up to #{shutdown_timeout}s for workers to finish..." }

      # The producer has no work to drain — kill it immediately. It may
      # be blocked in a streaming HTTP read that neither Thread#raise nor
      # Thread#kill can interrupt (the exception is deferred until the
      # next GVL checkpoint, which won't happen until data arrives on the
      # socket). Give it a brief moment to exit cleanly, then force-kill.
      producer_thread.kill unless producer_thread.join(1)

      # Workers get the full shutdown timeout to drain remaining jobs.
      join_with_deadline(worker_threads)

      # Drain any pending acks/nacks after all workers have exited.
      @ack_processor.stop(timeout: shutdown_timeout)

      logger.info { "Zizq worker stopped" }
    end

    private

    def install_signal_handlers #: () -> void
      %w[INT TERM].each do |signal|
        Signal.trap(signal) { shutdown }
      end
    end

    def start_producer_thread #: () -> Thread
      Thread.new do
        Thread.current.name = "zizq-producer"

        until @shutdown
          begin
            client = Zizq.client
            logger.info { "Connecting to #{client.url}..." }

            client.take_jobs(
              prefetch:,
              queues:,
              on_connect: -> {
                logger.info { "Connected. Listening for jobs." }
                @backoff.reset
              }
            ) do |job_hash|
              begin
                logger.debug { "Received #{job_hash.type} (#{job_hash.id}), dispatch queue: #{@dispatch_queue.size}" }
                @dispatch_queue.push(job_hash)
              rescue ClosedQueueError
                # Shutdown in progress — stop consuming from stream.
                # Exiting the block causes the HTTP connection to close,
                # and the server will requeue any unacknowledged jobs.
                break
              end
            end

            # Stream ended normally — reset backoff for next reconnect.
            @backoff.reset
          rescue Zizq::ConnectionError, Zizq::StreamError => e
            break if @shutdown

            logger.warn { "#{e.message}. Reconnecting in #{@backoff.duration}s..." }
            @backoff.wait
          rescue => e
            break if @shutdown

            logger.error { "Error: #{e.class}: #{e.message}" }
            logger.debug { e.backtrace&.join("\n") }
            @backoff.wait
          end
        end

        # Ensure queue is closed so workers can drain and exit
        @dispatch_queue.close rescue nil
        logger.info { "Stopped" }
      ensure
        # Wake the main thread whether we exited due to shutdown or a crash.
        @shutdown_latch.close rescue nil
      end
    end

    def start_worker_threads #: () -> Array[Thread]
      (0...thread_count).map do |thread_idx|
        Thread.new(thread_idx) do |tidx|
          Thread.current.name = "zizq-worker-#{tidx}"

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

      loop do
        # pop returns nil when queue is closed and empty
        job = @dispatch_queue.pop
        break if job.nil?

        dispatch(job, wid)
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
    # set job metadata, deserialize payload, call perform, then report success
    # or failure.
    #
    # The outer rescue Exception catches anything the inner rescue misses,
    # including non-StandardError exceptions and errors from malformed job
    # hashes. This keeps the worker thread alive regardless of what happens
    # during dispatch.
    def dispatch(job, worker_id) #: (Resources::Job, String?) -> void
      job_type = job.type
      job_id = job.id

      logger.debug { "Processing #{job_type} (#{job_id})" }

      # Resolve the job class from the type string and validate that it
      # includes Zizq::Job. Object.const_get naturally triggers
      # Zeitwerk/autoload if configured.
      job_class = begin
        Object.const_get(job_type)
      rescue NameError => e
        logger.error { "Unknown job type '#{job_type}' (#{job_id}): #{e.message}" }
        push_nack(job_id, e)
        return
      end

      unless job_class.is_a?(Class) && job_class.include?(Zizq::Job)
        logger.error { "#{job_type} (#{job_id}) does not include Zizq::Job" }
        push_nack(job_id, RuntimeError.new("#{job_type} does not include Zizq::Job"))
        return
      end

      # After the runtime guard above, we know job_class includes Zizq::Job.
      zizq_job_class = job_class #: Zizq::job_class

      job_instance = zizq_job_class.new
      job_instance.set_zizq_job(job)

      begin
        args, kwargs = zizq_job_class.zizq_deserialize(
          job.payload || { "args" => [], "kwargs" => {} }
        )

        t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        job_instance.perform(*args, **kwargs)
        elapsed_ms = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1000).round(1)

        push_ack(job_id)

        logger.debug { "Completed #{job_type} (#{job_id}) in #{elapsed_ms}ms" }
      rescue Exception => e # Intentionally rescuing all Exceptions here
        logger.error { "#{job_type} (#{job_id}) failed: #{e.class}: #{e.message}" }
        push_nack(job_id, e)
      end
    rescue Exception => e
      # Last resort in the case of something truly unexpected.
      logger.error { "Dispatch error (#{job_id || 'unknown'}): #{e.class}: #{e.message}" }
      push_nack(job_id, e) if job_id
    end

    # @rbs job_id: String
    # @rbs return: void
    def push_ack(job_id)
      @ack_processor.push(AckProcessor::Ack.new(job_id:))
    end

    # @rbs job_id: String
    # @rbs error: Exception
    # @rbs return: void
    def push_nack(job_id, error)
      @ack_processor.push(AckProcessor::Nack.new(
        job_id:     job_id,
        error:      "#{error.class}: #{error.message}",
        error_type: error.class.name,
        backtrace:  error.backtrace&.join("\n")
      ))
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

        logger.warn { "Shutdown timeout reached. Killing thread #{t.name}" }
        t.kill
      end
    end
  end
end
