# Copyright (c) 2026 Chris Corbyn <chris@zanxio.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

require "httpx"
require "msgpack"
require "json"
require "stringio"
require "uri"

module Zanxio
  # Low-level HTTP wrapper for the Zanxio job queue server API.
  #
  # Supports both JSON and MessagePack serialization formats, determined at
  # construction time.
  class Client
    CONTENT_TYPES = { #: Hash[Zanxio::format, String]
      msgpack: "application/msgpack",
      json: "application/json"
    }.freeze

    STREAM_ACCEPT = { #: Hash[Zanxio::format, String]
      msgpack: "application/vnd.zanxio.msgpack-stream",
      json: "application/x-ndjson"
    }.freeze

    # The base URL of the Zanxio server (e.g. "https://localhost:7890")
    attr_reader :url #: String

    # The message format to use for all communication between the client and
    # the server (default = `:msgpack`).
    attr_reader :format #: Zanxio::format

    # Initialize a new instance of the client with the given base URL and
    # optional format options.
    def initialize(url:, format: :msgpack) #: (url: String, ?format: Zanxio::format) -> void
      @url = url.chomp("/")
      @format = format

      # h2c upgrades the connection to HTTP/2 on the first GET request,
      # enabling multiplexing for all subsequent requests (including POSTs
      # for acks/nacks) on the same persistent connection.
      @http = HTTPX.plugin(:persistent).plugin(:stream).plugin(:h2c)
      @content_type = CONTENT_TYPES.fetch(format)
      @stream_accept = STREAM_ACCEPT.fetch(format)
    end

    # Enqueue a new job.
    #
    # This is a low-level primitive that makes a direct API call to the server
    # using the Zanxio API's expected inputs. Callers should generally use
    # [`Zanxio::enqueue`] instead.
    #
    # Returns a resource instance of the new job wrapping the API response.
    #
    # @rbs type: String
    # @rbs queue: String
    # @rbs payload: Hash[String | Symbol, untyped]
    # @rbs priority: Integer?
    # @rbs ready_at: Float?
    # @rbs retry_limit: Integer?
    # @rbs backoff: Hash[Symbol, untyped]?
    # @rbs return: Resources::Job
    def enqueue(type:, queue:, payload:, priority: nil, ready_at: nil, retry_limit: nil, backoff: nil)
      body = { type:, queue:, payload: } #: Hash[Symbol, untyped]
      body[:priority] = priority if priority
      # ready_at is fractional seconds in Ruby; the server expects ms.
      body[:ready_at] = (ready_at * 1000).to_i if ready_at
      body[:retry_limit] = retry_limit if retry_limit
      body[:backoff] = backoff if backoff

      response = post("/jobs", body)
      data = handle_response!(response, expected: 201)
      Resources::Job.new(self, data)
    end

    # Get a single job by ID.
    def get_job(id) #: (String) -> Resources::Job
      response = get("/jobs/#{id}")
      data = handle_response!(response, expected: 200)
      Resources::Job.new(self, data)
    end

    # List jobs with optional filters.
    #
    # Multi-value filters (`status`, `queue`, `type`) accept arrays — they
    # are joined with commas as the server expects.
    #
    # @rbs status: (String | Array[String])?
    # @rbs queue: (String | Array[String])?
    # @rbs type: (String | Array[String])?
    # @rbs from: String?
    # @rbs order: Zanxio::sort_direction?
    # @rbs limit: Integer?
    # @rbs return: Resources::JobPage
    def list_jobs(status: nil, queue: nil, type: nil, from: nil, order: nil, limit: nil)
      options = { status:, queue:, type:, from:, order:, limit: }.compact #: Hash[Symbol, untyped]
      params = build_list_params(options, multi_keys: %i[status queue type])
      response = get("/jobs", params:)
      data = handle_response!(response, expected: 200)
      Resources::JobPage.new(self, data)
    end

    # List error records for a job.
    #
    # @rbs id: String
    # @rbs from: String?
    # @rbs order: Zanxio::sort_direction?
    # @rbs limit: Integer?
    # @rbs return: Resources::ErrorPage
    def list_errors(id, from: nil, order: nil, limit: nil)
      params = { from:, order:, limit: }.compact #: Hash[Symbol, untyped]
      response = get("/jobs/#{id}/errors", params:)
      data = handle_response!(response, expected: 200)
      Resources::ErrorPage.new(self, data)
    end

    # Health check.
    def health #: () -> Hash[String, untyped]
      response = get("/health")
      handle_response!(response, expected: 200)
    end

    # Server version string.
    def server_version #: () -> String
      response = get("/version")
      data = handle_response!(response, expected: 200)
      data["version"]
    end

    # Mark a job as successfully completed (ack).
    #
    # If this method (or [`#report_failure`]) is not called upon job
    # completion, the Zanxio server will consider it in-flight and will not
    # send any more jobs if the prefetch limit has been reached, or the
    # server's global in-flight limit has been reached. Jobs must be either
    # acknowledged or failed before new jobs are sent.
    #
    # Jobs are durable and "at least once" delivery is guaranteed. If the
    # client disconnects before it is able to report success or failure the
    # server automatically moves the job back to the queue where it will be
    # provided to another worker. Clients should be prepared to see the same
    # job more than once for this reason.
    #
    # The Zanxio server sends heartbeat messages to connected workers so that
    # it can quickly detect and handle disconnected clients.
    def report_success(id) #: (String) -> nil
      response = raw_post("/jobs/#{id}/success")
      handle_response!(response, expected: 204)
      nil
    end

    # Report a job failure (nack).
    #
    # Returns the updated job metadata.
    #
    # If this method is not called when errors occur processing jobs, the
    # Zanxio server will consider it in-flight and will not send any more jobs
    # if the prefetch limit has been reached, or the server's global in-flight
    # limit has been reached. Jobs must be either acknowledged or failed before
    # new jobs are sent.
    #
    # Jobs are durable and "at least once" delivery is guaranteed. If the
    # client disconnects before it is able to report success or failure the
    # server automatically moves the job back to the queue where it will be
    # provided to another worker. Clients should be prepared to see the same
    # job more than once for this reason.
    #
    # The Zanxio server sends heartbeat messages to connected workers so that
    # it can quickly detect and handle disconnected clients.
    #
    # @rbs id: String
    # @rbs error: String
    # @rbs error_type: String?
    # @rbs backtrace: String?
    # @rbs retry_at: Float?
    # @rbs kill: bool
    # @rbs return: Resources::Job
    def report_failure(id, error:, error_type: nil, backtrace: nil, retry_at: nil, kill: false)
      body = { error: } #: Hash[Symbol, untyped]
      body[:error_type] = error_type if error_type
      body[:backtrace] = backtrace if backtrace
      # retry_at is fractional seconds in Ruby; the server expects ms.
      body[:retry_at] = (retry_at * 1000).to_i if retry_at
      body[:kill] = kill if kill

      response = post("/jobs/#{id}/failure", body)
      data = handle_response!(response, expected: 200)
      Resources::Job.new(self, data)
    end

    # Aliases for ack/nack vs report_success/report_failure.
    alias ack report_success
    alias nack report_failure

    # Stream jobs from the server. Yields parsed job hashes.
    #
    # This method does not return unless the server closes the connection or
    # the connection is otherwise interrupted. Jobs are continuously streamed
    # to the client, and when no jobs are available the client waits for new
    # jobs to become ready.
    #
    # If the client does not acknowledge or fail jobs with `[#report_success`]
    # or [`#report_failure`] the server will stop sending new jobs to the
    # client as it hits its prefetch limit.
    #
    # Jobs are durable and "at least once" delivery is guaranteed. If the
    # client disconnects before it is able to report success or failure the
    # server automatically moves the job back to the queue where it will be
    # provided to another worker. Clients should be prepared to see the same
    # job more than once for this reason.
    #
    # The Zanxio server sends periodic heartbeat messages to the client which
    # are silently consumed.
    #
    # Example:
    #
    #    client.take_jobs(prefetch: 5) do |job_hash|
    #      puts "Got job: #{job_hash.inspect}"
    #      client.ack(job_hash['id']) # mark the job completed
    #    end
    #
    # @rbs prefetch: Integer
    # @rbs queues: Array[String]
    # @rbs worker_id: String?
    # @rbs &block: (Resources::Job) -> void
    # @rbs return: void
    def take_jobs(prefetch: 1, queues: [], worker_id: nil, on_connect: nil, &block)
      raise ArgumentError, "take_jobs requires a block" unless block

      params = { prefetch: } #: Hash[Symbol, untyped]
      params[:queue] = queues.join(",") unless queues.empty?

      headers = { "Accept" => @stream_accept }
      headers["Worker-Id"] = worker_id if worker_id

      uri = build_uri("/jobs/take", params:)

      # stream: true returns an HTTPX::StreamResponse whose `each` yields
      # raw byte chunks as they arrive over the wire, rather than buffering
      # the entire response body into memory. The StreamResponse also calls
      # `raise_for_status` internally after the stream ends (or on HTTP
      # errors), so we don't need a manual status check.
      stream = @http.get(uri, headers:, stream: true)
      begin
        stream.status # force the connection; raises on failure
        on_connect&.call
      rescue StopIteration
        # Empty stream — connection closed before any data arrived.
        return
      end

      # Wrap each parsed hash in a Resources::Job before yielding.
      wrapper = proc { |data| block.call(Resources::Job.new(self, data)) }

      case @format
      when :json then self.class.parse_ndjson(stream, &wrapper)
      when :msgpack then self.class.parse_msgpack_stream(stream, &wrapper)
      end
    rescue HTTPX::HTTPError => e
      raise StreamError, "take jobs stream returned HTTP #{e.status}"
    rescue HTTPX::ConnectionError => e
      raise ConnectionError, e.message
    rescue IOError, Errno::ECONNRESET, Errno::EPIPE => e
      raise StreamError, e.message
    end

    # Parse an NDJSON stream from an enumerable of byte chunks.
    #
    # Buffers chunks and splits on newline boundaries. The buffer only
    # ever holds one partial line between extractions, so the `slice!`
    # cost is trivial. Empty lines (heartbeats) are silently skipped.
    def self.parse_ndjson(chunks) #: (Enumerable[String]) { (Hash[String, untyped]) -> void } -> void
      buffer = +""
      chunks.each do |chunk|
        buffer << chunk
        while (idx = buffer.index("\n"))
          line = buffer.slice!(0, idx + 1) #: String
          line.strip!
          next if line.empty?
          yield JSON.parse(line)
        end
      end
    end

    # Parse a length-prefixed MessagePack stream from an enumerable of byte
    # chunks.
    #
    # Format: [4-byte big-endian length][MsgPack payload].
    # A zero-length frame is a heartbeat and is silently skipped.
    #
    # Uses StringIO for efficient position-based reading rather than
    # repeatedly slicing from the front of a String (which copies all
    # remaining bytes on every extraction).
    def self.parse_msgpack_stream(chunks) #: (Enumerable[String]) { (Hash[String, untyped]) -> void } -> void
      io = StringIO.new("".b)

      chunks.each do |chunk|
        # Append new data at the end, then return to the read position.
        read_pos = io.pos
        io.seek(0, IO::SEEK_END)
        io.write(chunk.b)
        io.seek(read_pos)

        # Extract complete frames.
        while io.size - io.pos >= 4
          len_bytes = io.read(4) #: String
          len = len_bytes.unpack1("N") #: Integer

          if len == 0 # heartbeat
            next
          end

          if io.size - io.pos < len
            # Incomplete frame — rewind past the length header and wait
            # for more data.
            io.seek(-4, IO::SEEK_CUR)
            break
          end

          yield MessagePack.unpack(io.read(len))
        end

        # Compact: discard already-consumed bytes so the StringIO doesn't
        # grow without bound over the life of the stream.
        remaining = io.read
        io = StringIO.new(remaining || "".b)
      end
    end

    # GET a path on the server and return the decoded response body.
    #
    # The path should include any query parameters already (e.g. pagination
    # links from the server's `pages` object). This is intentionally public
    # so that resource objects like Page can follow links without resorting
    # to `.send`.
    def get_path(path) #: (String) -> Hash[String, untyped]
      response = @http.get(
        "#{@url}#{path}",
        headers: { "Accept" => @content_type }
      )
      handle_response!(response, expected: 200)
    end

    private

    # Build a full URI string with optional query parameters.
    def build_uri(path, params: {}) #: (String, ?params: Hash[Symbol, untyped]) -> String
      uri = "#{@url}#{path}"
      unless params.empty?
        uri = "#{uri}?#{URI.encode_www_form(params)}"
      end
      uri
    end

    # Build query params for list endpoints, joining multi-value keys with ",".
    def build_list_params(options, multi_keys: []) #: (Hash[Symbol, untyped], ?multi_keys: Array[Symbol]) -> Hash[Symbol, untyped]
      params = {} #: Hash[Symbol, untyped]
      options.each do |key, value|
        if multi_keys.include?(key) && value.is_a?(Array)
          params[key] = value.join(",")
        else
          params[key] = value
        end
      end
      params
    end

    def encode_body(body) #: (Hash[Symbol, untyped]) -> String
      case @format
      when :msgpack then MessagePack.pack(body)
      when :json then JSON.generate(body)
      else raise ArgumentError, "Unknown format: #{@format}"
      end
    end

    def decode_body(data) #: (String) -> Hash[String, untyped]
      case @format
      when :msgpack then MessagePack.unpack(data)
      when :json then JSON.parse(data)
      else raise ArgumentError, "Unknown format: #{@format}"
      end
    end

    def get(path, params: {}) #: (String, ?params: Hash[Symbol, untyped]) -> HTTPX::Response
      @http.get(
        build_uri(path, params:),
        headers: { "Accept" => @content_type }
      )
    end

    def post(path, body) #: (String, Hash[Symbol, untyped]) -> HTTPX::Response
      @http.post(
        build_uri(path),
        headers: { "Content-Type" => @content_type, "Accept" => @content_type },
        body: encode_body(body)
      )
    end

    def raw_post(path) #: (String) -> HTTPX::Response
      @http.post(
        build_uri(path),
        headers: { "Accept" => @content_type }
      )
    end

    # Check response status and decode body, raising on errors.
    def handle_response!(response, expected:) #: (HTTPX::Response, expected: Integer) -> Hash[String, untyped]?
      # HTTPX returns an ErrorResponse on connection failure
      if response.is_a?(HTTPX::ErrorResponse)
        raise ConnectionError, response.error.message
      end

      status = response.status

      if status == expected
        return nil if status == 204
        decode_body(response.body.to_s)
      else
        body = begin
          decode_body(response.body.to_s)
        rescue
          nil
        end
        message = body&.fetch("error", nil) || "HTTP #{status}"
        error_class = case status
                      when 404 then NotFoundError
                      when 400..499 then ClientError
                      when 500..599 then ServerError
                      else ResponseError
                      end
        raise error_class.new(message, status: status, body: body)
      end
    end
  end
end
