# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# frozen_string_literal: true

require "test_helper"

class TestClient < Minitest::Test
  URL = "http://localhost:7890"

  def setup
    @json_client = Zizq::Client.new(url: URL, format: :json)
    @msgpack_client = Zizq::Client.new(url: URL, format: :msgpack)
  end

  def teardown
    @json_client.close
    @msgpack_client.close
  end

  # --- enqueue ---

  def test_enqueue_json
    job_response = { "id" => "abc123", "type" => "SendEmail", "queue" => "emails",
                     "priority" => 32_768, "status" => "ready", "ready_at" => 1000,
                     "attempts" => 0 }

    stub_request(:post, "#{URL}/jobs")
      .with(
        body: JSON.generate({ type: "SendEmail", queue: "emails", payload: { user_id: 42 } }),
        headers: { "Content-Type" => "application/json", "Accept" => "application/json" }
      )
      .to_return(status: 201, body: JSON.generate(job_response),
                 headers: { "Content-Type" => "application/json" })

    result = @json_client.enqueue(type: "SendEmail", queue: "emails", payload: { user_id: 42 })
    assert_instance_of Zizq::Resources::Job, result
    assert_equal "abc123", result.id
    assert_equal "SendEmail", result.type
  end

  def test_enqueue_msgpack
    job_response = { "id" => "abc123", "type" => "SendEmail", "queue" => "emails",
                     "priority" => 32_768, "status" => "ready", "ready_at" => 1000,
                     "attempts" => 0 }

    stub_request(:post, "#{URL}/jobs")
      .with(
        body: MessagePack.pack({ type: "SendEmail", queue: "emails", payload: { user_id: 42 } }),
        headers: { "Content-Type" => "application/msgpack", "Accept" => "application/msgpack" }
      )
      .to_return(status: 201, body: MessagePack.pack(job_response),
                 headers: { "Content-Type" => "application/msgpack" })

    result = @msgpack_client.enqueue(type: "SendEmail", queue: "emails", payload: { user_id: 42 })
    assert_instance_of Zizq::Resources::Job, result
    assert_equal "abc123", result.id
  end

  def test_enqueue_with_priority
    stub_request(:post, "#{URL}/jobs")
      .with { |req| JSON.parse(req.body)["priority"] == 100 }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    @json_client.enqueue(type: "Job", queue: "q", payload: {}, priority: 100)
  end

  def test_enqueue_with_ready_at
    stub_request(:post, "#{URL}/jobs")
      .with { |req| JSON.parse(req.body)["ready_at"] == 9_999_000 }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    # 9999.0 seconds → 9_999_000 ms on the wire
    @json_client.enqueue(type: "Job", queue: "q", payload: {}, ready_at: 9999.0)
  end

  def test_enqueue_with_time_ready_at
    now = Time.now
    ready_at = now + 60

    stub_request(:post, "#{URL}/jobs")
      .with { |req| JSON.parse(req.body)["ready_at"] == (ready_at.to_f * 1000).to_i }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    @json_client.enqueue(type: "Job", queue: "q", payload: {}, ready_at: ready_at)
  end

  def test_enqueue_400_raises_client_error
    stub_request(:post, "#{URL}/jobs")
      .to_return(status: 400, body: JSON.generate({ "error" => "queue is required" }),
                 headers: { "Content-Type" => "application/json" })

    err = assert_raises(Zizq::ClientError) do
      @json_client.enqueue(type: "", queue: "", payload: {})
    end
    assert_equal 400, err.status
    assert_equal "queue is required", err.message
  end

  # --- enqueue_bulk ---

  def test_enqueue_bulk_json
    jobs_response = { "jobs" => [
      { "id" => "j1", "type" => "SendEmail", "queue" => "emails", "status" => "ready" },
      { "id" => "j2", "type" => "ProcessOrder", "queue" => "orders", "status" => "ready" }
    ] }

    stub_request(:post, "#{URL}/jobs/bulk")
      .with(
        body: JSON.generate({
          jobs: [
            { type: "SendEmail", queue: "emails", payload: { user_id: 42 } },
            { type: "ProcessOrder", queue: "orders", payload: { order_id: 7 } }
          ]
        }),
        headers: { "Content-Type" => "application/json", "Accept" => "application/json" }
      )
      .to_return(status: 201, body: JSON.generate(jobs_response),
                 headers: { "Content-Type" => "application/json" })

    result = @json_client.enqueue_bulk(jobs: [
      { type: "SendEmail", queue: "emails", payload: { user_id: 42 } },
      { type: "ProcessOrder", queue: "orders", payload: { order_id: 7 } }
    ])
    assert_instance_of Array, result
    assert_equal 2, result.size
    assert_instance_of Zizq::Resources::Job, result[0]
    assert_equal "j1", result[0].id
    assert_equal "j2", result[1].id
  end

  def test_enqueue_bulk_msgpack
    jobs_response = { "jobs" => [
      { "id" => "j1", "type" => "SendEmail", "queue" => "emails", "status" => "ready" }
    ] }

    stub_request(:post, "#{URL}/jobs/bulk")
      .with(
        body: MessagePack.pack({
          jobs: [{ type: "SendEmail", queue: "emails", payload: { user_id: 42 } }]
        }),
        headers: { "Content-Type" => "application/msgpack", "Accept" => "application/msgpack" }
      )
      .to_return(status: 201, body: MessagePack.pack(jobs_response),
                 headers: { "Content-Type" => "application/msgpack" })

    result = @msgpack_client.enqueue_bulk(jobs: [
      { type: "SendEmail", queue: "emails", payload: { user_id: 42 } }
    ])
    assert_instance_of Array, result
    assert_equal 1, result.size
    assert_instance_of Zizq::Resources::Job, result[0]
    assert_equal "j1", result[0].id
  end

  def test_enqueue_bulk_400_raises_client_error
    stub_request(:post, "#{URL}/jobs/bulk")
      .to_return(status: 400, body: JSON.generate({ "error" => "invalid job type" }),
                 headers: { "Content-Type" => "application/json" })

    err = assert_raises(Zizq::ClientError) do
      @json_client.enqueue_bulk(jobs: [{ type: "", queue: "", payload: {} }])
    end
    assert_equal 400, err.status
    assert_equal "invalid job type", err.message
  end

  def test_enqueue_bulk_converts_ready_at_to_ms
    stub_request(:post, "#{URL}/jobs/bulk")
      .with { |req|
        body = JSON.parse(req.body)
        body["jobs"][0]["ready_at"] == 9_999_000
      }
      .to_return(status: 201, body: JSON.generate({ "jobs" => [{ "id" => "x" }] }),
                 headers: { "Content-Type" => "application/json" })

    @json_client.enqueue_bulk(jobs: [
      { type: "Job", queue: "q", payload: {}, ready_at: 9999.0 }
    ])
  end

  # --- get_job ---

  def test_get_job
    job = { "id" => "job1", "type" => "Foo", "queue" => "default",
            "priority" => 32_768, "status" => "ready", "ready_at" => 1000,
            "attempts" => 0, "payload" => { "key" => "value" } }

    stub_request(:get, "#{URL}/jobs/job1")
      .with(headers: { "Accept" => "application/json" })
      .to_return(status: 200, body: JSON.generate(job),
                 headers: { "Content-Type" => "application/json" })

    result = @json_client.get_job("job1")
    assert_instance_of Zizq::Resources::Job, result
    assert_equal "job1", result.id
    assert_equal({ "key" => "value" }, result.payload)
  end

  def test_get_job_not_found
    stub_request(:get, "#{URL}/jobs/missing")
      .to_return(status: 404, body: JSON.generate({ "error" => "not found" }),
                 headers: { "Content-Type" => "application/json" })

    assert_raises(Zizq::NotFoundError) { @json_client.get_job("missing") }
  end

  # --- list_jobs ---

  def test_list_jobs_no_filters
    response = { "jobs" => [], "pages" => { "self" => "/jobs" } }

    stub_request(:get, "#{URL}/jobs")
      .with(headers: { "Accept" => "application/json" })
      .to_return(status: 200, body: JSON.generate(response),
                 headers: { "Content-Type" => "application/json" })

    result = @json_client.list_jobs
    assert_instance_of Zizq::Resources::JobPage, result
    assert_equal [], result.jobs
  end

  def test_list_jobs_with_filters
    response = { "jobs" => [{ "id" => "j1" }], "pages" => { "self" => "/jobs" } }

    stub_request(:get, "#{URL}/jobs?status=ready,in_flight&queue=emails&limit=10")
      .to_return(status: 200, body: JSON.generate(response),
                 headers: { "Content-Type" => "application/json" })

    result = @json_client.list_jobs(status: %w[ready in_flight], queue: "emails", limit: 10)
    assert_equal 1, result.jobs.size
  end

  # --- get_error ---

  def test_get_error
    response = { "attempt" => 2, "message" => "timeout",
                 "error_type" => "Timeout::Error", "backtrace" => nil,
                 "dequeued_at" => 1000, "failed_at" => 2000 }

    stub_request(:get, "#{URL}/jobs/j1/errors/2")
      .to_return(status: 200, body: JSON.generate(response),
                 headers: { "Content-Type" => "application/json" })

    result = @json_client.get_error("j1", attempt: 2)
    assert_instance_of Zizq::Resources::ErrorRecord, result
    assert_equal 2, result.attempt
    assert_equal "timeout", result.message
    assert_equal "Timeout::Error", result.error_type
  end

  # --- list_errors ---

  def test_list_errors
    response = { "errors" => [
      { "attempt" => 1, "message" => "boom", "failed_at" => 2000 }
    ], "pages" => { "self" => "/jobs/j1/errors" } }

    stub_request(:get, "#{URL}/jobs/j1/errors")
      .to_return(status: 200, body: JSON.generate(response),
                 headers: { "Content-Type" => "application/json" })

    result = @json_client.list_errors("j1")
    assert_instance_of Zizq::Resources::ErrorPage, result
    assert_equal 1, result.errors.size
    assert_equal "boom", result.errors[0].message
  end

  def test_list_errors_with_options
    response = { "errors" => [], "pages" => { "self" => "/jobs/j1/errors" } }

    stub_request(:get, "#{URL}/jobs/j1/errors?order=desc&limit=5")
      .to_return(status: 200, body: JSON.generate(response),
                 headers: { "Content-Type" => "application/json" })

    @json_client.list_errors("j1", order: :desc, limit: 5)
  end

  # --- health ---

  def test_health
    stub_request(:get, "#{URL}/health")
      .to_return(status: 200, body: JSON.generate({ "status" => "ok" }),
                 headers: { "Content-Type" => "application/json" })

    result = @json_client.health
    assert_equal "ok", result["status"]
  end

  # --- server_version ---

  def test_server_version
    stub_request(:get, "#{URL}/version")
      .to_return(status: 200, body: JSON.generate({ "version" => "0.1.0" }),
                 headers: { "Content-Type" => "application/json" })

    assert_equal "0.1.0", @json_client.server_version
  end

  # --- report_success (ack) ---

  def test_report_success
    stub_request(:post, "#{URL}/jobs/job1/success")
      .with(headers: { "Accept" => "application/json" })
      .to_return(status: 204, body: "")

    result = @json_client.report_success("job1")
    assert_nil result
  end

  def test_ack_alias
    stub_request(:post, "#{URL}/jobs/job1/success")
      .to_return(status: 204, body: "")

    @json_client.ack("job1")
  end

  def test_report_success_404
    stub_request(:post, "#{URL}/jobs/missing/success")
      .to_return(status: 404, body: JSON.generate({ "error" => "not found" }),
                 headers: { "Content-Type" => "application/json" })

    assert_raises(Zizq::NotFoundError) { @json_client.report_success("missing") }
  end

  # --- report_success_bulk (bulk ack) ---

  def test_report_success_bulk
    stub_request(:post, "#{URL}/jobs/success")
      .with(
        body: JSON.generate({ ids: ["j1", "j2"] }),
        headers: { "Content-Type" => "application/json", "Accept" => "application/json" }
      )
      .to_return(status: 204, body: "")

    result = @json_client.report_success_bulk(["j1", "j2"])
    assert_nil result
  end

  def test_report_success_bulk_422_accepted
    stub_request(:post, "#{URL}/jobs/success")
      .to_return(status: 422, body: JSON.generate({ "not_found" => ["j2"] }),
                 headers: { "Content-Type" => "application/json" })

    result = @json_client.report_success_bulk(["j1", "j2"])
    assert_nil result
  end

  def test_report_success_bulk_500_raises
    stub_request(:post, "#{URL}/jobs/success")
      .to_return(status: 500, body: JSON.generate({ "error" => "internal" }),
                 headers: { "Content-Type" => "application/json" })

    assert_raises(Zizq::ServerError) { @json_client.report_success_bulk(["j1"]) }
  end

  def test_ack_bulk_alias
    stub_request(:post, "#{URL}/jobs/success")
      .to_return(status: 204, body: "")

    result = @json_client.ack_bulk(["j1"])
    assert_nil result
  end

  def test_report_success_bulk_msgpack
    stub_request(:post, "#{URL}/jobs/success")
      .with(
        body: MessagePack.pack({ ids: ["j1", "j2"] }),
        headers: { "Content-Type" => "application/msgpack", "Accept" => "application/msgpack" }
      )
      .to_return(status: 204, body: "")

    result = @msgpack_client.report_success_bulk(["j1", "j2"])
    assert_nil result
  end

  # --- report_failure (nack) ---

  def test_report_failure
    updated_job = { "id" => "job1", "status" => "scheduled", "attempts" => 1 }

    stub_request(:post, "#{URL}/jobs/job1/failure")
      .with { |req|
        body = JSON.parse(req.body)
        body["message"] == "RuntimeError: boom" &&
          body["error_type"] == "RuntimeError" &&
          body["backtrace"] == "line1\nline2"
      }
      .to_return(status: 200, body: JSON.generate(updated_job),
                 headers: { "Content-Type" => "application/json" })

    result = @json_client.report_failure("job1",
                                         message: "RuntimeError: boom",
                                         error_type: "RuntimeError",
                                         backtrace: "line1\nline2")
    assert_instance_of Zizq::Resources::Job, result
    assert_equal "scheduled", result.status
    assert_equal 1, result.attempts
  end

  def test_report_failure_with_kill
    stub_request(:post, "#{URL}/jobs/job1/failure")
      .with { |req| JSON.parse(req.body)["kill"] == true }
      .to_return(status: 200, body: JSON.generate({ "id" => "job1", "status" => "dead" }),
                 headers: { "Content-Type" => "application/json" })

    result = @json_client.report_failure("job1", message: "fatal", kill: true)
    assert_equal "dead", result.status
  end

  def test_nack_alias
    stub_request(:post, "#{URL}/jobs/job1/failure")
      .to_return(status: 200, body: JSON.generate({ "id" => "job1" }),
                 headers: { "Content-Type" => "application/json" })

    @json_client.nack("job1", message: "oops")
  end

  # --- error handling ---

  def test_500_raises_server_error
    stub_request(:get, "#{URL}/health")
      .to_return(status: 500, body: JSON.generate({ "error" => "internal" }),
                 headers: { "Content-Type" => "application/json" })

    err = assert_raises(Zizq::ServerError) { @json_client.health }
    assert_equal 500, err.status
  end

  # --- take (NDJSON streaming) ---

  def test_take_ndjson_yields_jobs
    job1 = { "id" => "j1", "type" => "Foo", "queue" => "default" }
    job2 = { "id" => "j2", "type" => "Bar", "queue" => "default" }
    body = "#{JSON.generate(job1)}\n\n#{JSON.generate(job2)}\n"

    stub_request(:get, "#{URL}/jobs/take?prefetch=5")
      .with(headers: { "Accept" => "application/x-ndjson" })
      .to_return(status: 200, body: body,
                 headers: { "Content-Type" => "application/x-ndjson" })

    jobs = []
    @json_client.take_jobs(prefetch: 5) { |job| jobs << job }
    assert_equal 2, jobs.size
    assert_instance_of Zizq::Resources::Job, jobs[0]
    assert_equal "j1", jobs[0].id
    assert_equal "j2", jobs[1].id
  end

  def test_take_ndjson_skips_heartbeats
    job1 = { "id" => "j1" }
    # Heartbeat lines are empty
    body = "\n\n#{JSON.generate(job1)}\n\n\n"

    stub_request(:get, "#{URL}/jobs/take?prefetch=1")
      .to_return(status: 200, body: body,
                 headers: { "Content-Type" => "application/x-ndjson" })

    jobs = []
    @json_client.take_jobs(prefetch: 1) { |job| jobs << job }
    assert_equal 1, jobs.size
  end

  def test_take_with_queues
    stub_request(:get, "#{URL}/jobs/take?prefetch=1&queue=emails,webhooks")
      .to_return(status: 200, body: "",
                 headers: { "Content-Type" => "application/x-ndjson" })

    @json_client.take_jobs(prefetch: 1, queues: %w[emails webhooks]) { |_| }
  end

  def test_take_with_worker_id
    stub_request(:get, "#{URL}/jobs/take?prefetch=1")
      .with(headers: { "Worker-Id" => "myworker-1" })
      .to_return(status: 200, body: "",
                 headers: { "Content-Type" => "application/x-ndjson" })

    @json_client.take_jobs(prefetch: 1, worker_id: "myworker-1") { |_| }
  end

  def test_take_requires_block
    assert_raises(ArgumentError) { @json_client.take_jobs(prefetch: 1) }
  end

  def test_take_on_connect_called_when_stream_opens
    body = "#{JSON.generate({ "id" => "j1" })}\n"

    stub_request(:get, "#{URL}/jobs/take?prefetch=1")
      .to_return(status: 200, body: body,
                 headers: { "Content-Type" => "application/x-ndjson" })

    connected = false
    @json_client.take_jobs(prefetch: 1, on_connect: -> { connected = true }) { |_| }
    assert connected, "on_connect should have been called"
  end

  def test_take_on_connect_called_for_empty_stream
    stub_request(:get, "#{URL}/jobs/take?prefetch=1")
      .to_return(status: 200, body: "",
                 headers: { "Content-Type" => "application/x-ndjson" })

    connected = false
    @json_client.take_jobs(prefetch: 1, on_connect: -> { connected = true }) { |_| }
    assert connected, "on_connect should fire when a 200 is received (server was reachable)"
  end

  # --- take (MsgPack streaming) ---

  def test_take_msgpack_yields_jobs
    job1 = { "id" => "j1", "type" => "Foo" }
    job2 = { "id" => "j2", "type" => "Bar" }
    packed1 = MessagePack.pack(job1)
    packed2 = MessagePack.pack(job2)

    body = +""
    body << [packed1.bytesize].pack("N") << packed1
    # Heartbeat (zero-length frame)
    body << [0].pack("N")
    body << [packed2.bytesize].pack("N") << packed2

    stub_request(:get, "#{URL}/jobs/take?prefetch=2")
      .with(headers: { "Accept" => "application/vnd.zizq.msgpack-stream" })
      .to_return(status: 200, body: body,
                 headers: { "Content-Type" => "application/vnd.zizq.msgpack-stream" })

    jobs = []
    @msgpack_client.take_jobs(prefetch: 2) { |job| jobs << job }
    assert_equal 2, jobs.size
    assert_instance_of Zizq::Resources::Job, jobs[0]
    assert_equal "j1", jobs[0].id
    assert_equal "j2", jobs[1].id
  end

  def test_take_msgpack_skips_heartbeats
    body = +""
    # Two heartbeats, no jobs
    body << [0].pack("N")
    body << [0].pack("N")

    stub_request(:get, "#{URL}/jobs/take?prefetch=1")
      .to_return(status: 200, body: body,
                 headers: { "Content-Type" => "application/vnd.zizq.msgpack-stream" })

    jobs = []
    @msgpack_client.take_jobs(prefetch: 1) { |job| jobs << job }
    assert_equal 0, jobs.size
  end

  # --- parser unit tests (class methods) ---

  def test_parse_ndjson_single_chunk
    job1 = { "id" => "j1" }
    job2 = { "id" => "j2" }
    chunks = ["#{JSON.generate(job1)}\n\n#{JSON.generate(job2)}\n"]

    jobs = []
    Zizq::Client.parse_ndjson(chunks) { |job| jobs << job }
    assert_equal 2, jobs.size
    assert_equal "j1", jobs[0]["id"]
    assert_equal "j2", jobs[1]["id"]
  end

  def test_parse_ndjson_split_across_chunks
    # Simulate a line split mid-JSON across two chunks
    full_line = JSON.generate({ "id" => "j1" })
    chunk1 = full_line[0, 5]
    chunk2 = "#{full_line[5..]}\n"

    jobs = []
    Zizq::Client.parse_ndjson([chunk1, chunk2]) { |job| jobs << job }
    assert_equal 1, jobs.size
    assert_equal "j1", jobs[0]["id"]
  end

  def test_parse_ndjson_skips_blank_lines
    jobs = []
    Zizq::Client.parse_ndjson(["\n\n\n"]) { |job| jobs << job }
    assert_equal 0, jobs.size
  end

  def test_parse_msgpack_stream_single_chunk
    job1 = { "id" => "j1" }
    job2 = { "id" => "j2" }
    packed1 = MessagePack.pack(job1)
    packed2 = MessagePack.pack(job2)

    data = +""
    data << [packed1.bytesize].pack("N") << packed1
    data << [0].pack("N") # heartbeat
    data << [packed2.bytesize].pack("N") << packed2

    jobs = []
    Zizq::Client.parse_msgpack_stream([data]) { |job| jobs << job }
    assert_equal 2, jobs.size
    assert_equal "j1", jobs[0]["id"]
    assert_equal "j2", jobs[1]["id"]
  end

  def test_parse_msgpack_stream_split_across_chunks
    packed = MessagePack.pack({ "id" => "j1" })
    header = [packed.bytesize].pack("N")

    # Split the frame across two chunks: header in first, payload in second
    jobs = []
    Zizq::Client.parse_msgpack_stream([header, packed]) { |job| jobs << job }
    assert_equal 1, jobs.size
    assert_equal "j1", jobs[0]["id"]
  end

  def test_parse_msgpack_stream_heartbeats_only
    data = +""
    data << [0].pack("N")
    data << [0].pack("N")

    jobs = []
    Zizq::Client.parse_msgpack_stream([data]) { |job| jobs << job }
    assert_equal 0, jobs.size
  end

  # --- url normalization ---

  def test_trailing_slash_stripped
    client = Zizq::Client.new(url: "http://localhost:7890/", format: :json)
    assert_equal "http://localhost:7890", client.url
  ensure
    client&.close
  end
end
