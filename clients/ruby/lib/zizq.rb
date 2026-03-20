# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

require_relative "zizq/version"
require_relative "zizq/error"
require_relative "zizq/configuration"

# Autoloaded when first accessed — avoids loading heavy deps at require time.
autoload :MessagePack, "msgpack"

module Zizq
  autoload :AckProcessor,   "zizq/ack_processor"
  autoload :Backoff,        "zizq/backoff"
  autoload :BulkEnqueue,    "zizq/bulk_enqueue"
  autoload :Client,         "zizq/client"
  autoload :EnqueueOptions, "zizq/enqueue_options"
  autoload :Job,            "zizq/job"
  autoload :Lifecycle,      "zizq/lifecycle"
  autoload :Resources,      "zizq/resources"
  autoload :Worker,         "zizq/worker"

  @client_mutex = Mutex.new

  class << self
    # Returns the client configuration.
    #
    # The configuration can be updated by calling [`Zizq::configure`].
    #
    # This configuration is for the client only. Worker parameters are
    # configured on a per-run basis for flexibility.
    def configuration #: () -> Configuration
      @configuration ||= Configuration.new
    end

    # Yields the global configuration ready for updates, which should be done
    # during application initialization, before any jobs are enqueued or
    # worked.
    #
    #   Zizq.configure do |c|
    #     c.url = "http://localhost:7890"
    #     c.format = :msgpack
    #   end
    def configure #: () { (Configuration) -> void } -> void
      yield configuration
    ensure
      @client = nil # shared client is potentially stale
    end

    # Returns a shared client instance built from the global configuration.
    #
    # The client is memoized so that persistent HTTP connections are reused
    # across calls, reducing TCP connection overhead.
    def client #: () -> Client
      @client ||= begin
        @client_mutex.synchronize do
          break @client if @client

          configuration.validate!
          @client = Client.new(
            url: configuration.url,
            format: configuration.format,
            ssl_context: configuration.ssl_context
          )
        end
      end
    end

    # Resets all global state: configuration and shared client.
    # Intended for use in tests.
    def reset! #: () -> void
      @client&.close
      @client = nil
      @configuration = nil
    end

    # Enqueue a job by class with positional and keyword arguments.
    #
    # By default all arguments are serialized as JSON, which means hashes with
    # symbol keys will become hashes with string keys. The serialization
    # behaviour can be changed by implementing `::zizq_serialize` and
    # `::zizq_deserialize` as class methods on the job class.
    #
    # Default job options can be overridden at enqueue-time by providing a
    # block which receives a mutable `Zizq::EnqueueOptions` instance.
    #
    #   Zizq.enqueue(SendEmailJob, 42, template: "welcome")
    #   Zizq.enqueue(SendEmailJob, 42) { |o| o.queue = "priority" }
    #
    # Job classes may also override `::zizq_enqueue_options` to implement
    # dynamically computed options, such as dynamic prioritisation. This class
    # method accepts the same arguments as the `#perform` method and returns an
    # instance of `Zizq::EnqueueOptions`. Any overrides may call `super` and
    # modify the result.
    #
    #   class SendEmailJob
    #     include Zizq::Job
    #
    #     zizq_priority 1000
    #
    #     def self.zizq_enqueue_options(user_id, template:)
    #       opts = super
    #       opts.priority /= 2 if template == "welcome"
    #       opts
    #     end
    #
    #     def perform(user_id, template:)
    #       # ...
    #     end
    #   end
    #
    # @rbs job_class: Class & Zizq::job_class
    # @rbs args: Array[untyped]
    # @rbs kwargs: Hash[Symbol, untyped]
    # @rbs &block: ?(EnqueueOptions) -> void
    # @rbs return: Resources::Job
    def enqueue(job_class, *args, **kwargs, &block)
      client.enqueue(**build_enqueue_params(job_class, *args, **kwargs, &block))
    end

    # Enqueue multiple jobs atomically in a single bulk request.
    #
    # This can significantly imprive throughput when many jobs need to be
    # enqueued collectively. There is no upper limit on the number of jobs in
    # the request though generally it is probably wise to keep this to less
    # than 1000 jobs unless you have strong atomicity requuirements for a
    # larger number of jobs..
    #
    # Yields a builder object whose `#enqueue` method accepts the same
    # arguments as `Zizq.enqueue`. All collected jobs are sent as a
    # single `POST /jobs/bulk` request and an array of jobs is returned in the
    # same order as the inputs.
    #
    #   Zizq.enqueue_bulk do |b|
    #     b.enqueue(ProcessPaymentJob, 7)
    #     b.enqueue(SendEmailJob, 42, template: "welcome")
    #     b.enqueue(SendEmailJob, 42) { |o| o.queue = "priority" }
    #   end
    #
    # @rbs &block: (BulkEnqueue) -> void
    # @rbs return: Array[Resources::Job]
    def enqueue_bulk(&block)
      builder = BulkEnqueue.new
      yield builder
      jobs_params = builder.jobs
      return [] if jobs_params.empty?
      client.enqueue_bulk(jobs: jobs_params)
    end

    # @api private
    # Build the params hash for a single enqueue call.
    #
    # @rbs job_class: Class & Zizq::job_class
    # @rbs args: Array[untyped]
    # @rbs kwargs: Hash[Symbol, untyped]
    # @rbs &block: ?(EnqueueOptions) -> void
    # @rbs return: Hash[Symbol, untyped]
    def build_enqueue_params(job_class, *args, **kwargs, &block)
      unless job_class.is_a?(Class) && job_class < Zizq::Job
        raise ArgumentError, "#{job_class.inspect} must include Zizq::Job"
      end

      # After the runtime guard above, we know job_class includes Zizq::Job
      # and therefore has ClassMethods extended. Assert this for steep.
      zizq_job_class = job_class #: Zizq::job_class

      type = zizq_job_class.name
      raise ArgumentError, "Cannot enqueue anonymous class" if type.nil?

      opts = zizq_job_class.zizq_enqueue_options(*args, **kwargs)
      yield opts if block_given?

      payload = zizq_job_class.zizq_serialize(*args, **kwargs)

      params = { type:, queue: opts.queue, payload: } #: Hash[Symbol, untyped]
      params[:priority] = opts.priority if opts.priority
      params[:ready_at] = opts.ready_at if opts.ready_at
      params[:retry_limit] = opts.retry_limit if opts.retry_limit

      # Backoff times are specified in seconds in Ruby but the server
      # expects milliseconds. Convert here at the boundary.
      if opts.backoff
        backoff = opts.backoff
        params[:backoff] = {
          exponent: backoff[:exponent].to_f,
          base_ms: (backoff[:base].to_f * 1000).to_f,
          jitter_ms: (backoff[:jitter].to_f * 1000).to_f
        }
      end

      # Retention times are specified in seconds in Ruby but the server
      # expects milliseconds. Convert here at the boundary.
      if opts.retention
        retention = opts.retention
        wire = {} #: Hash[Symbol, Integer]
        wire[:completed_ms] = (retention[:completed].to_f * 1000).to_i if retention[:completed]
        wire[:dead_ms] = (retention[:dead].to_f * 1000).to_i if retention[:dead]
        params[:retention] = wire
      end

      # Support ActiveSupport::Duration and Numeric alike.
      # Both ready_at and delay are in fractional seconds; the Client
      # handles the conversion to the server's millisecond format.
      params[:ready_at] = Time.now.to_f + opts.delay.to_f if opts.delay

      params
    end
  end
end
