# Copyright (c) 2026 Chris Corbyn <chris@zanxio.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

require_relative "zanxio/version"
require_relative "zanxio/error"
require_relative "zanxio/configuration"

# Autoloaded when first accessed — avoids loading heavy deps at require time.
autoload :HTTPX, "httpx"
autoload :MessagePack, "msgpack"

module Zanxio
  autoload :Client,    "zanxio/client"
  autoload :Job,       "zanxio/job"
  autoload :Resources, "zanxio/resources"
  autoload :Worker,    "zanxio/worker"

  class << self
    # Returns the client configuration.
    #
    # The configuration can be updated by calling [`Zanxio::configure`].
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
    #   Zanxio.configure do |c|
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
        configuration.validate!
        Client.new(url: configuration.url, format: configuration.format)
      end
    end

    # Resets all global state: configuration and shared client.
    # Intended for use in tests.
    def reset! #: () -> void
      @client = nil
      @configuration = nil
    end

    # Convenience method to enqueue a job by class with a given payload.
    #
    # Keyword options provided to `Zanxio::enqueue` override options specified
    # in the job class (which override the default options set on the server).
    #
    #   Zanxio.enqueue(SendEmailJob, { user_id: 42 })
    #   Zanxio.enqueue(SendEmailJob, { user_id: 42 }, queue: "priority", priority: 100)
    #   Zanxio.enqueue(SendEmailJob, { user_id: 42 }, delay: 60)
    #
    # @rbs job_class: Class
    # @rbs payload: Hash[String | Symbol, untyped]
    # @rbs queue: String?
    # @rbs priority: Integer?
    # @rbs delay: (Float | Integer)?
    # @rbs ready_at: Float?
    # @rbs retry_limit: Integer?
    # @rbs backoff: Zanxio::backoff?
    # @rbs retention: Zanxio::retention?
    # @rbs return: Hash[String, untyped]
    def enqueue(
      job_class,
      payload,
      queue: nil,
      priority: nil,
      delay: nil,
      ready_at: nil,
      retry_limit: nil,
      backoff: nil,
      retention: nil
    )
      unless job_class.is_a?(Class) && job_class < Zanxio::Job
        raise ArgumentError, "#{job_class.inspect} must include Zanxio::Job"
      end

      # After the runtime guard above, we know job_class includes Zanxio::Job
      # and therefore has ClassMethods extended. Assert this for steep.
      zanxio_job_class = job_class #: Zanxio::job_class

      type = zanxio_job_class.name
      raise ArgumentError, "Cannot enqueue anonymous class" if type.nil?

      queue ||= zanxio_job_class.zanxio_queue
      retry_limit ||= zanxio_job_class.zanxio_retry_limit
      backoff ||= zanxio_job_class.zanxio_backoff
      retention ||= zanxio_job_class.zanxio_retention

      params = { type:, queue:, payload: } #: Hash[Symbol, untyped]
      params[:priority] = priority if priority
      params[:ready_at] = ready_at if ready_at
      params[:retry_limit] = retry_limit if retry_limit

      # Backoff times are specified in seconds in Ruby but the server
      # expects milliseconds. Convert here at the boundary.
      if backoff
        params[:backoff] = {
          exponent: backoff[:exponent].to_f,
          base_ms: (backoff[:base].to_f * 1000).to_f,
          jitter_ms: (backoff[:jitter].to_f * 1000).to_f
        }
      end

      # Retention times are specified in seconds in Ruby but the server
      # expects milliseconds. Convert here at the boundary.
      if retention
        wire = {} #: Hash[Symbol, Integer]
        wire[:completed_ms] = (retention[:completed].to_f * 1000).to_i if retention[:completed]
        wire[:dead_ms] = (retention[:dead].to_f * 1000).to_i if retention[:dead]
        params[:retention] = wire
      end

      # Support ActiveSupport::Duration and Numeric alike.
      # Both ready_at and delay are in fractional seconds; the Client
      # handles the conversion to the server's millisecond format.
      params[:ready_at] = Time.now.to_f + delay.to_f if delay

      client.enqueue(**params)
    end
  end
end
