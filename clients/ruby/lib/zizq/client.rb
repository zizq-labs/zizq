# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# rbs_inline: enabled
# frozen_string_literal: true

require "async"
require "async/barrier"
require "async/http/client"
require "async/http/endpoint"
require "protocol/http/body/buffered"
require "msgpack"
require "json"
require "stringio"
require "uri"
require "weakref"

module Zizq
  # Low-level HTTP wrapper for the Zizq job queue server API.
  #
  # Supports both JSON and MessagePack serialization formats, determined at
  # construction time.
  #
  # HTTP requests are dispatched through a persistent background IO thread
  # when called from non-Async contexts, keeping the HTTP/2 connection alive
  # across calls and avoiding ephemeral port exhaustion. When called from
  # within an existing Async reactor, the shared HTTP client is used directly.
  class Client
    # A fully-read HTTP response (status + decoded body), safe to use outside
    # the async reactor that produced it.
    RawResponse = Data.define(:status, :body)

    CONTENT_TYPES = { #: Hash[Zizq::format, String]
      msgpack: "application/msgpack",
      json: "application/json"
    }.freeze

    STREAM_ACCEPT = { #: Hash[Zizq::format, String]
      msgpack: "application/vnd.zizq.msgpack-stream",
      json: "application/x-ndjson"
    }.freeze

    # The base URL of the Zizq server (e.g. "https://localhost:7890")
    attr_reader :url #: String

    # The message format to use for all communication between the client and
    # the server (default = `:msgpack`).
    attr_reader :format #: Zizq::format

    # Initialize a new instance of the client with the given base URL and
    # optional format options.
    #
    # @rbs url: String
    # @rbs format: Zizq::format
    # @rbs ssl_context: OpenSSL::SSL::SSLContext?
    # @rbs return: void
    def initialize(url:, format: :msgpack, ssl_context: nil)
      @url = url.chomp("/")
      @format = format

      endpoint_options = { protocol: Async::HTTP::Protocol::HTTP2 } #: Hash[Symbol, untyped]
      endpoint_options[:ssl_context] = ssl_context if ssl_context

      @endpoint = Async::HTTP::Endpoint.parse(
        @url,
        **endpoint_options,
      )

      @io_mutex = Mutex.new
      @io_thread = nil #: Thread?
      @io_queue = nil #: Thread::Queue?

      # Each thread gets its own Async::HTTP::Client bound to its own
      # reactor. We track them all so close can shut them down.
      @http_clients = [] #: Array[Async::HTTP::Client]
      @http_clients_mutex = Mutex.new
      @http_key = :"zizq_http_#{object_id}"

      @content_type = CONTENT_TYPES.fetch(format)
      @stream_accept = STREAM_ACCEPT.fetch(format)
    end

    # Close all thread-local HTTP clients and release connections.
    def close #: () -> void
      if @io_thread&.alive?
        @io_queue&.close
        @io_thread&.join
      end

      @http_clients_mutex.synchronize do
        @http_clients.each do |ref|
          ref.close
        rescue WeakRef::RefError
          # Client already GC'd (owning thread exited).
        rescue NoMethodError
          # The async connection pool may hold references to tasks whose
          # fibers were already reclaimed when their owning Sync reactor
          # exited. Stopping those dead tasks raises NoMethodError; safe
          # to ignore.
        end
        @http_clients.clear
      end
    end

    # Enqueue a new job.
    #
    # This is a low-level primitive that makes a direct API call to the server
    # using the Zizq API's expected inputs. Callers should generally use
    # [`Zizq::enqueue`] instead.
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
    # @rbs retention: Hash[Symbol, untyped]?
    # @rbs return: Resources::Job
    def enqueue(type:, queue:, payload:, priority: nil, ready_at: nil, retry_limit: nil, backoff: nil, retention: nil)
      body = { type:, queue:, payload: } #: Hash[Symbol, untyped]
      body[:priority] = priority if priority
      # ready_at is fractional seconds in Ruby; the server expects ms.
      body[:ready_at] = (ready_at.to_f * 1000).to_i if ready_at
      body[:retry_limit] = retry_limit if retry_limit
      body[:backoff] = backoff if backoff
      body[:retention] = retention if retention

      response = post("/jobs", body)
      data = handle_response!(response, expected: 201)
      Resources::Job.new(self, data)
    end

    # Enqueue multiple jobs atomically in a single bulk request.
    #
    # This is a low-level primitive that makes a direct API call to the server
    # using the Zizq API's expected inputs. Callers should generally use
    # [`Zizq::enqueue_bulk`] instead.
    #
    # Returns an array of resource instances wrapping the API response.
    #
    # @rbs jobs: Array[Hash[Symbol, untyped]]
    # @rbs return: Array[Resources::Job]
    def enqueue_bulk(jobs:)
      body = { jobs: jobs.map { |j|
        wire = { type: j[:type], queue: j[:queue], payload: j[:payload] } #: Hash[Symbol, untyped]
        wire[:priority] = j[:priority] if j[:priority]
        # ready_at is fractional seconds in Ruby; the server expects ms.
        wire[:ready_at] = (j[:ready_at].to_f * 1000).to_i if j[:ready_at]
        wire[:retry_limit] = j[:retry_limit] if j[:retry_limit]
        wire[:backoff] = j[:backoff] if j[:backoff]
        wire[:retention] = j[:retention] if j[:retention]
        wire
      }}

      response = post("/jobs/bulk", body)
      data = handle_response!(response, expected: 201)
      data["jobs"].map { |j| Resources::Job.new(self, j) }
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
    # @rbs order: Zizq::sort_direction?
    # @rbs limit: Integer?
    # @rbs return: Resources::JobPage
    def list_jobs(status: nil, queue: nil, type: nil, from: nil, order: nil, limit: nil)
      options = { status:, queue:, type:, from:, order:, limit: }.compact #: Hash[Symbol, untyped]
      params = build_list_params(options, multi_keys: %i[status queue type])
      response = get("/jobs", params:)
      data = handle_response!(response, expected: 200)
      Resources::JobPage.new(self, data)
    end

    # Get a single error record by job ID and attempt number.
    #
    # @rbs id: String
    # @rbs attempt: Integer
    # @rbs return: Resources::ErrorRecord
    def get_error(id, attempt:)
      response = get("/jobs/#{id}/errors/#{attempt}")
      data = handle_response!(response, expected: 200)
      Resources::ErrorRecord.new(self, data)
    end

    # List error records for a job.
    #
    # @rbs id: String
    # @rbs from: String?
    # @rbs order: Zizq::sort_direction?
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
    # completion, the Zizq server will consider it in-flight and will not
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
    # The Zizq server sends heartbeat messages to connected workers so that
    # it can quickly detect and handle disconnected clients.
    def report_success(id) #: (String) -> nil
      response = raw_post("/jobs/#{id}/success")
      handle_response!(response, expected: 204)
      nil
    end

    # Bulk-mark jobs as successfully completed (bulk ack).
    #
    # See [`#report_success`] for full details of how acknowledgemen works.
    #
    # There are two ways in which the server can respond successfully:
    #
    # 1. 204 - No Content (All jobs acknowledged)
    # 2. 422 - Unprocessible Entity (Some jobs were not found)
    #
    # Both of these statuses are in reality treated as success because missing
    # jobs have either been previously acknowledged and purged, or moved to
    # some other status that cannot be acknowledged.
    #
    # Other error response types will still raise.
    #
    # @rbs ids: Array[String]
    # @rbs return: nil
    def report_success_bulk(ids)
      response = post("/jobs/success", { ids: ids })
      return nil if response.status == 422
      handle_response!(response, expected: 204)
    end

    alias ack_bulk report_success_bulk

    # Report a job failure (nack).
    #
    # Returns the updated job metadata.
    #
    # If this method is not called when errors occur processing jobs, the
    # Zizq server will consider it in-flight and will not send any more jobs
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
    # The Zizq server sends heartbeat messages to connected workers so that
    # it can quickly detect and handle disconnected clients.
    #
    # @rbs id: String
    # @rbs message: String
    # @rbs error_type: String?
    # @rbs backtrace: String?
    # @rbs retry_at: Float?
    # @rbs kill: bool
    # @rbs return: Resources::Job
    def report_failure(id, message:, error_type: nil, backtrace: nil, retry_at: nil, kill: false)
      body = { message: } #: Hash[Symbol, untyped]
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
    # The Zizq server sends periodic heartbeat messages to the client which are
    # silently consumed.
    #
    # Example:
    #
    #    client.take_jobs(prefetch: 5) do |job|
    #      puts "Got job: #{job.inspect}"
    #      client.ack(job.id) # mark the job completed
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

      path = build_path("/jobs/take", params:)
      headers = { "accept" => @stream_accept }
      headers["worker-id"] = worker_id if worker_id

      Sync do
        response = http.get(path, headers)

        begin
          raise StreamError, "take jobs stream returned HTTP #{response.status}" unless response.status == 200
          on_connect&.call

          # Wrap each parsed hash in a Resources::Job before yielding.
          wrapper = proc { |data| block.call(Resources::Job.new(self, data)) }

          case @format
          when :json then self.class.parse_ndjson(response.body, &wrapper)
          when :msgpack then self.class.parse_msgpack_stream(response.body, &wrapper)
          end
        ensure
          response.close rescue nil
        end
      end
    rescue SocketError, IOError, EOFError, Errno::ECONNRESET, Errno::EPIPE,
           OpenSSL::SSL::SSLError => e
      raise ConnectionError, e.message
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
      response = request { |http| consume_response(http.get(path, {"accept" => @content_type})) }
      handle_response!(response, expected: 200)
    end

    private

    # Build a relative path with optional query parameters.
    def build_path(path, params: {}) #: (String, ?params: Hash[Symbol, untyped]) -> String
      unless params.empty?
        path = "#{path}?#{URI.encode_www_form(params)}"
      end
      path
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

    # Dispatch a block to the appropriate execution context.
    #
    # If already inside an Async reactor (e.g. AckProcessor, producer),
    # yields the calling thread's HTTP client directly. Otherwise,
    # dispatches via the persistent background IO thread.
    def request(&block) #: () { (Async::HTTP::Client) -> RawResponse } -> RawResponse
      if Async::Task.current?
        yield http
      else
        sync_call(&block)
      end
    end

    # Read the response body and close it, returning a RawResponse that is
    # safe to use outside the reactor.
    def consume_response(response) #: (untyped) -> RawResponse
      RawResponse.new(status: response.status, body: response.read)
    ensure
      response.close
    end

    # Push a work block to the background IO thread and block until it
    # completes, returning the result or re-raising any exception.
    def sync_call(&block) #: () { (Async::HTTP::Client) -> RawResponse } -> RawResponse
      ensure_io_thread

      result_queue = Thread::Queue.new
      @io_queue.push([block, result_queue])

      tag, value = result_queue.pop
      if tag == :ok
        value
      else
        raise value
      end
    rescue ClosedQueueError
      raise ConnectionError, "client is closed"
    end

    # Lazily start the background IO thread (double-checked locking).
    def ensure_io_thread #: () -> void
      return if @io_thread&.alive?

      @io_mutex.synchronize do
        return if @io_thread&.alive?

        @io_queue = Thread::Queue.new
        @io_thread = Thread.new { io_thread_run }
        @io_thread.name = "zizq-io"
      end
    end

    # Main loop for the background IO thread. Mirrors AckProcessor: runs an
    # Async reactor, pops work from the queue (fiber-scheduler-aware), and
    # dispatches each call as a concurrent fiber via a barrier.
    def io_thread_run #: () -> void
      Sync do
        barrier = Async::Barrier.new

        while (item = @io_queue.pop)
          block, result_queue = item
          barrier.async do
            result_queue.push([:ok, block.call(http)])
          rescue => e
            result_queue.push([:error, e])
          end
        end

        barrier.wait
      end
    end

    # Return the calling thread's HTTP client, creating one if needed.
    # Uses thread_variable_get/set (not Thread.current[]) because the
    # latter is fiber-local — each Async fiber would get its own client.
    # The tracking array holds WeakRefs so clients from exited threads
    # can be garbage-collected.
    def http #: () -> Async::HTTP::Client
      Thread.current.thread_variable_get(@http_key) || begin
        client = Async::HTTP::Client.new(@endpoint)
        @http_clients_mutex.synchronize do
          @http_clients.reject! { |ref| !ref.weakref_alive? }
          @http_clients << WeakRef.new(client)
        end
        Thread.current.thread_variable_set(@http_key, client)
        client
      end
    end

    def get(path, params: {}) #: (String, ?params: Hash[Symbol, untyped]) -> RawResponse
      request do |http|
        consume_response(
          http.get(
            build_path(path, params:),
            {"accept" => @content_type}
          )
        )
      end
    end

    def post(path, body) #: (String, Hash[Symbol, untyped]) -> RawResponse
      request do |http|
        consume_response(
          http.post(
            build_path(path),
            {"content-type" => @content_type, "accept" => @content_type},
            Protocol::HTTP::Body::Buffered.wrap(encode_body(body))
          )
        )
      end
    end

    def raw_post(path) #: (String) -> RawResponse
      request do |http|
        consume_response(
          http.post(
            build_path(path),
            {"accept" => @content_type}
          )
        )
      end
    end

    # Check response status and decode body, raising on errors.
    def handle_response!(response, expected:) #: (RawResponse, expected: Integer) -> Hash[String, untyped]?
      status = response.status

      if status == expected
        return nil if status == 204
        decode_body(response.body)
      else
        body = begin
          decode_body(response.body)
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
