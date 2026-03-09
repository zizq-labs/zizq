# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

module Zizq
  # Mixin which all valid job classes must include.
  #
  # This module must be included in a class to make it a valid Zizq job. The
  # class name becomes the job type, and the worker resolves types back to
  # classes via `Object.const_get` (which naturally triggers any autoload
  # logic).
  #
  #   class SendEmailJob
  #     include Zizq::Job
  #
  #     zizq_queue "emails"   # optional, defaults to "default"
  #
  #     def perform(user_id, template:)
  #       puts "Sending #{template} email to user #{user_id}"
  #     end
  #   end
  #
  # The job can be configured through class methods to set the queue, priority
  # etc. Classes can also override `::zizq_enqueue_options` to implement
  # dynamically configured jobs based on their arguments.
  module Job
    def self.included(base) #: (Class) -> void
      base.extend(ClassMethods)
    end

    module ClassMethods
      # Declare the default queue for this job class.
      #
      # If not called, defaults to "default". Jobs enqueued for this class will
      # use the specified queue unless explicitly overridden during
      # [`Zizq::enqueue`] or by overriding `::zizq_enqueue_options` on the job
      # class.
      def zizq_queue(name = nil) #: (?String?) -> String
        if name
          @zizq_queue = name
        else
          @zizq_queue || "default"
        end
      end

      # Declare the default priority for this job class.
      #
      # If not called, defaults to the default priority on the Zizq server.
      # Jobs enqueued for this class will use the specified priority unless
      # explicitly overridden during [`Zizq::enqueue`] or by overriding
      # `::zizq_enqueue_options` on the job class.
      def zizq_priority(priority = nil) #: (?Integer?) -> Integer?
        if priority
          @zizq_priority = priority
        else
          @zizq_priority
        end
      end

      # Declare the default retry limit for this job class.
      #
      # The job may fail up to the number of times specified by the retry limit
      # and will exponentially backoff. Once the retry limit is reached, the
      # job is killed and becomes part of the dead set.
      #
      # If not configured, the server's default is used.
      def zizq_retry_limit(limit = nil) #: (?Integer?) -> Integer?
        if limit
          @zizq_retry_limit = limit
        else
          @zizq_retry_limit
        end
      end

      # Declare the default backoff configuration for this job class.
      #
      # Times are specified in seconds (optionally fractional).
      # In a Rails app `ActiveSupport::Duration` is supported too.
      #
      # All three parameters must be specified together and are used in the
      # following exponential backoff formula:
      #
      #   delay = base + attempts**exponent + rand(0.0..jitter)*attempts
      #
      # Example:
      #
      #   zizq_backoff exponent: 4.0, base: 15, jitter: 30
      #
      # If not configured, the server's default backoff policy is used.
      def zizq_backoff(exponent: nil, base: nil, jitter: nil) #: (?exponent: Numeric?, ?base: Numeric?, ?jitter: Numeric?) -> Zizq::backoff?
        if exponent || base || jitter
          unless exponent && base && jitter
            raise ArgumentError, "all of exponent:, base:, jitter: are required"
          end

          @zizq_backoff = { exponent: exponent.to_f, base: base.to_f, jitter: jitter.to_f }
        else
          @zizq_backoff
        end
      end

      # Declare the default retention configuration for this job class.
      #
      # Times are specified in seconds (optionally fractional).
      # In a Rails app `ActiveSupport::Duration` is supported too.
      #
      # Both parameters are optional — only the ones provided will be sent
      # to the server. Omitted values use the server's defaults.
      #
      # Example:
      #
      #   zizq_retention completed: 0, dead: 7 * 86_400
      #
      # If not configured, the server's default is used.
      def zizq_retention(completed: nil, dead: nil) #: (?completed: Numeric?, ?dead: Numeric?) -> Zizq::retention?
        if completed || dead
          result = {} #: Hash[Symbol, Float]

          result[:completed] = completed.to_f if completed
          result[:dead] = dead.to_f if dead

          @zizq_retention = result
        else
          @zizq_retention
        end
      end

      # Serialize positional and keyword arguments for the `#perform` method
      # into a payload hash suitable for sending to the server.
      #
      # The result must be a JSON encodable Hash.
      #
      # The default implementation generates a hash of the form:
      #
      #   { "args" => [ 42, "Hello" ], "kwargs" => { "template": "example" } }
      #
      # If you override this method you almost certainly need to override
      # `::zizq_deserialize` too. Any failure to deserialize the arguments will
      # cause the job to fail and backoff according to the backoff policy.
      def zizq_serialize(*args, **kwargs) #: (*untyped, **untyped) -> Hash[String, untyped]
        { "args" => args, "kwargs" => kwargs.transform_keys(&:to_s) }
      end

      # Deserialize a payload hash back into positional and keyword arguments.
      #
      # The payload is a JSON decoded Hash.
      #
      # The default implementation receives a Hash of the form:
      #
      #   { "args" => [ 42, "Hello" ], "kwargs" => { "template": "example" } }
      #
      # And returns an array for `args` and `kwargs` of the form:
      #
      #   [ [ 42, "Hello" ], {template: "example"} ]
      #
      # Because the default implementation uses a JSON decoded Hash, any symbol
      # keys that were present at enqueue-time will be string keys after
      # decoding.
      #
      # Any failure to deserialize the arguments will cause the job to fail and
      # backoff according to the backoff policy.
      def zizq_deserialize(payload) #: (Hash[String, untyped]) -> [Array[untyped], Hash[Symbol, untyped]]
        args   = payload.fetch("args")
        kwargs = payload.fetch("kwargs").transform_keys(&:to_sym)
        [args, kwargs]
      end

      # Build a `Zizq::EnqueueOptions` instance from the class-level job config.
      #
      # Subclasses can override this to implement dynamic logic such as
      # priority based on arguments:
      #
      #   def self.zizq_enqueue_options(user_id, template:)
      #     opts = super
      #     opts.priority = 0 if template == "urgent"
      #     opts
      #   end
      def zizq_enqueue_options(*args, **kwargs) #: (*untyped, **untyped) -> EnqueueOptions
        EnqueueOptions.new(
          queue:       zizq_queue,
          priority:    zizq_priority,
          retry_limit: zizq_retry_limit,
          backoff:     zizq_backoff,
          retention:   zizq_retention
        )
      end
    end

    # This is your job's main entrypoint when it is run by the worker.
    #
    # Override this method in your job class to define the work to perform.
    # Declare any positional and keyword arguments your job needs.
    #
    # Strong recommendation: stick to keyword arguments because they are much
    # easier to evolve over time in a backwards compatible way with any already
    # enqueued jobs.
    def perform(*args, **kwargs) #: (*untyped, **untyped) -> void
      raise NotImplementedError, "#{self.class.name}#perform must be implemented"
    end

    # --- Metadata helpers ---
    #
    # These delegate to the Resources::Job instance set by the worker
    # before calling #perform, giving the job access to its server-side
    # metadata.

    # The unique job ID assigned by the server.
    def zizq_id = @zizq_job&.id         #: () -> String?

    # How many times this job has previously been attempted (0 on the first
    # run, 1 on the second, etc...).
    def zizq_attempts = @zizq_job&.attempts   #: () -> Integer?

    # The queue this job was dequeued from.
    def zizq_queue = @zizq_job&.queue      #: () -> String?

    # The priority this job was enqueued with.
    def zizq_priority = @zizq_job&.priority   #: () -> Integer?

    # Time at which this job was dequeued (fractional seconds since the Unix
    # epoch). This can be converted to `Time` by using `Time.at(dequeued_at)`
    # but that is intentionally left to the caller due to time zone
    # considerations.
    def zizq_dequeued_at = @zizq_job&.dequeued_at #: () -> Float?

    # @api private
    # Set by the worker before calling #perform. Receives the full
    # Resources::Job object so all metadata is available through delegation.
    def set_zizq_job(job) #: (Resources::Job) -> void
      @zizq_job = job
    end
  end
end
