# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

module Zizq
  # Mixin which all valid job classes must include.
  #
  # Include this module in a class to make it a valid Zizq job. The class
  # name becomes the job type, and the worker resolves types back to classes
  # via `Object.const_get` (which naturally triggers any Zeitwerk/autoload
  # logic).
  #
  #   class SendEmailJob
  #     include Zizq::Job
  #
  #     zizq_queue "emails"   # optional, defaults to "default"
  #
  #     def perform(payload)
  #       puts "Sending email to user #{payload['user_id']}"
  #     end
  #   end
  #
  module Job
    def self.included(base) #: (Class) -> void
      base.extend(ClassMethods)
    end

    module ClassMethods
      # Declare the default queue for this job class.
      #
      # If not called, defaults to "default". Jobs enqueued for this class will
      # use the specified queue unless explicitly overridden in
      # [`Zizq::enqueue`].
      def zizq_queue(name = nil) #: (?String?) -> String
        if name
          @zizq_queue = name
        else
          @zizq_queue || "default"
        end
      end

      # Declare the default retry limit for this job class.
      #
      # The job may fail up to the number of times specified by the retry limit
      # and will exponentially backoff. Once the retry limit is reached, the
      # job is killed and becomes part of the dead set.
      #
      # If not called, the server's default is used.
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
      # `ActiveSupport::Duration` works here.
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
      # If not called, the server's default is used.
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
      # `ActiveSupport::Duration` works here.
      #
      # Both parameters are optional — only the ones provided will be sent
      # to the server. Omitted values use the server's defaults.
      #
      # Example:
      #
      #   zizq_retention completed: 0, dead: 7 * 86_400
      #
      # If not called, the server's default is used.
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
    end

    # Override this method in your job class to define the work to perform.
    # The worker calls this with the parsed payload hash.
    def perform(payload) #: (Hash[String, untyped]) -> void
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
    # considerations. Already in seconds (converted from ms by Resources::Job).
    def zizq_dequeued_at = @zizq_job&.dequeued_at #: () -> Float?

    # @api private
    # Set by the worker before calling #perform. Receives the full
    # Resources::Job object so all metadata is available through delegation.
    def set_zizq_job(job) #: (Resources::Job) -> void
      @zizq_job = job
    end
  end
end
