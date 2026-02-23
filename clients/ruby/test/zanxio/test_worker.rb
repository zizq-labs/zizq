# Copyright (c) 2026 Chris Corbyn <chris@zanxio.io>
# Licensed under the MIT License. See LICENSE file for details.

# frozen_string_literal: true

require "test_helper"

# A simple job that records what it received.
class RecordingJob
  include Zanxio::Job

  zanxio_queue "test"

  # Class-level storage so we can inspect results from the test.
  @results = []

  class << self
    attr_accessor :results
  end

  def perform(payload)
    self.class.results << {
      payload: payload,
      id: zanxio_id,
      attempts: zanxio_attempts,
      queue: zanxio_queue,
      priority: zanxio_priority
    }
  end
end

# A job that raises on perform.
class FailingJob
  include Zanxio::Job

  @fail_count = 0

  class << self
    attr_accessor :fail_count
  end

  def perform(_payload)
    self.class.fail_count += 1
    raise RuntimeError, "boom"
  end
end

class TestWorker < Minitest::Test
  URL = "http://localhost:7890"

  def setup
    Zanxio.reset!
    Zanxio.configure { |c| c.url = URL; c.format = :json }
    RecordingJob.results = []
    FailingJob.fail_count = 0
  end

  def test_defaults
    worker = Zanxio::Worker.new
    assert_equal 5, worker.thread_count
    assert_equal 1, worker.fiber_count
    assert_equal 5, worker.prefetch
    assert_equal [], worker.queues
    assert_equal 30, worker.shutdown_timeout
    assert_equal 1, worker.reconnect_interval
    assert_equal 30, worker.max_reconnect_interval
  end

  def test_custom_prefetch
    worker = Zanxio::Worker.new(thread_count: 4, fiber_count: 2, prefetch: 10)
    assert_equal 10, worker.prefetch
  end

  def test_default_prefetch_is_threads_times_fibers
    worker = Zanxio::Worker.new(thread_count: 4, fiber_count: 3)
    assert_equal 12, worker.prefetch
  end

  def test_dispatches_job_successfully
    job1 = { "id" => "j1", "type" => "RecordingJob", "queue" => "test",
             "priority" => 100, "attempts" => 1, "dequeued_at" => 1000,
             "payload" => { "user_id" => 42 } }

    # Return the job on first request, then empty body on subsequent ones.
    # This prevents the producer reconnect loop from re-delivering the job.
    stub_request(:get, "#{URL}/jobs/take?prefetch=1")
      .to_return(
        { status: 200, body: "#{JSON.generate(job1)}\n",
          headers: { "Content-Type" => "application/x-ndjson" } },
        { status: 200, body: "",
          headers: { "Content-Type" => "application/x-ndjson" } }
      )

    ack_stub = stub_request(:post, "#{URL}/jobs/j1/success")
      .to_return(status: 204)

    worker = Zanxio::Worker.new(thread_count: 1, queues: [], prefetch: 1,
                                 logger: Logger.new(File::NULL))

    t = Thread.new { worker.run }

    # Wait for the job to be processed
    deadline = Time.now + 5
    sleep 0.05 while RecordingJob.results.empty? && Time.now < deadline

    # Trigger shutdown
    worker.instance_variable_set(:@shutdown, true)
    worker.instance_variable_get(:@dispatch_queue).close rescue nil
    t.join(5)

    assert_equal 1, RecordingJob.results.size
    result = RecordingJob.results.first
    assert_equal({ "user_id" => 42 }, result[:payload])
    assert_equal "j1", result[:id]
    assert_equal 1, result[:attempts]
    assert_equal "test", result[:queue]
    assert_equal 100, result[:priority]

    assert_requested(ack_stub, at_least_times: 1)
  end

  def test_nacks_failing_job
    job1 = { "id" => "j1", "type" => "FailingJob", "queue" => "default",
             "priority" => 32_768, "attempts" => 1, "payload" => {} }

    stub_request(:get, "#{URL}/jobs/take?prefetch=1")
      .to_return(
        { status: 200, body: "#{JSON.generate(job1)}\n",
          headers: { "Content-Type" => "application/x-ndjson" } },
        { status: 200, body: "",
          headers: { "Content-Type" => "application/x-ndjson" } }
      )

    nack_stub = stub_request(:post, "#{URL}/jobs/j1/failure")
      .with { |req|
        body = JSON.parse(req.body)
        body["error"].include?("RuntimeError") &&
          body["error"].include?("boom") &&
          body["error_type"] == "RuntimeError"
      }
      .to_return(status: 200, body: JSON.generate({ "id" => "j1", "status" => "scheduled" }),
                 headers: { "Content-Type" => "application/json" })

    worker = Zanxio::Worker.new(thread_count: 1, prefetch: 1,
                                 logger: Logger.new(File::NULL))

    t = Thread.new { worker.run }

    deadline = Time.now + 5
    sleep 0.05 while FailingJob.fail_count == 0 && Time.now < deadline

    worker.instance_variable_set(:@shutdown, true)
    worker.instance_variable_get(:@dispatch_queue).close rescue nil
    t.join(5)

    assert_equal 1, FailingJob.fail_count
    assert_requested(nack_stub, at_least_times: 1)
  end

  def test_nacks_unknown_job_type
    job1 = { "id" => "j1", "type" => "NonExistentJobClass", "queue" => "default",
             "priority" => 32_768, "attempts" => 1, "payload" => {} }

    stub_request(:get, "#{URL}/jobs/take?prefetch=1")
      .to_return(
        { status: 200, body: "#{JSON.generate(job1)}\n",
          headers: { "Content-Type" => "application/x-ndjson" } },
        { status: 200, body: "",
          headers: { "Content-Type" => "application/x-ndjson" } }
      )

    # Should nack without kill — just a normal failure so backoff retry kicks in
    nack_stub = stub_request(:post, "#{URL}/jobs/j1/failure")
      .with { |req|
        body = JSON.parse(req.body)
        body["error_type"] == "NameError" && !body.key?("kill")
      }
      .to_return(status: 200, body: JSON.generate({ "id" => "j1", "status" => "scheduled" }),
                 headers: { "Content-Type" => "application/json" })

    worker = Zanxio::Worker.new(thread_count: 1, prefetch: 1,
                                 logger: Logger.new(File::NULL))

    t = Thread.new { worker.run }

    # Wait for the nack to happen
    deadline = Time.now + 5
    nacked = false
    until nacked || Time.now > deadline
      sleep 0.05
      begin
        assert_requested(nack_stub, at_least_times: 1)
        nacked = true
      rescue Minitest::Assertion
        # not yet
      end
    end

    worker.instance_variable_set(:@shutdown, true)
    worker.instance_variable_get(:@dispatch_queue).close rescue nil
    t.join(5)

    assert_requested(nack_stub, at_least_times: 1)
  end

  def test_dispatches_job_with_fibers
    job1 = { "id" => "j1", "type" => "RecordingJob", "queue" => "test",
             "priority" => 100, "attempts" => 1, "dequeued_at" => 1000,
             "payload" => { "user_id" => 42 } }

    stub_request(:get, "#{URL}/jobs/take?prefetch=2")
      .to_return(
        { status: 200, body: "#{JSON.generate(job1)}\n",
          headers: { "Content-Type" => "application/x-ndjson" } },
        { status: 200, body: "",
          headers: { "Content-Type" => "application/x-ndjson" } }
      )

    ack_stub = stub_request(:post, "#{URL}/jobs/j1/success")
      .to_return(status: 204)

    # 1 thread with 2 fibers exercises the Async code path
    worker = Zanxio::Worker.new(thread_count: 1, fiber_count: 2, prefetch: 2,
                                 logger: Logger.new(File::NULL))

    t = Thread.new { worker.run }

    deadline = Time.now + 5
    sleep 0.05 while RecordingJob.results.empty? && Time.now < deadline

    worker.instance_variable_set(:@shutdown, true)
    worker.instance_variable_get(:@dispatch_queue).close rescue nil
    t.join(5)

    assert_equal 1, RecordingJob.results.size
    result = RecordingJob.results.first
    assert_equal({ "user_id" => 42 }, result[:payload])
    assert_equal "j1", result[:id]
    assert_requested(ack_stub, at_least_times: 1)
  end

  def test_nacks_failing_job_with_fibers
    job1 = { "id" => "j1", "type" => "FailingJob", "queue" => "default",
             "priority" => 32_768, "attempts" => 1, "payload" => {} }

    stub_request(:get, "#{URL}/jobs/take?prefetch=2")
      .to_return(
        { status: 200, body: "#{JSON.generate(job1)}\n",
          headers: { "Content-Type" => "application/x-ndjson" } },
        { status: 200, body: "",
          headers: { "Content-Type" => "application/x-ndjson" } }
      )

    nack_stub = stub_request(:post, "#{URL}/jobs/j1/failure")
      .with { |req|
        body = JSON.parse(req.body)
        body["error"].include?("RuntimeError") &&
          body["error"].include?("boom")
      }
      .to_return(status: 200, body: JSON.generate({ "id" => "j1", "status" => "scheduled" }),
                 headers: { "Content-Type" => "application/json" })

    worker = Zanxio::Worker.new(thread_count: 1, fiber_count: 2, prefetch: 2,
                                 logger: Logger.new(File::NULL))

    t = Thread.new { worker.run }

    deadline = Time.now + 5
    sleep 0.05 while FailingJob.fail_count == 0 && Time.now < deadline

    worker.instance_variable_set(:@shutdown, true)
    worker.instance_variable_get(:@dispatch_queue).close rescue nil
    t.join(5)

    assert_equal 1, FailingJob.fail_count
    assert_requested(nack_stub, at_least_times: 1)
  end

  def test_multiple_fibers_process_concurrently
    job1 = { "id" => "j1", "type" => "RecordingJob", "queue" => "test",
             "priority" => 100, "attempts" => 1, "dequeued_at" => 1000,
             "payload" => { "n" => 1 } }
    job2 = { "id" => "j2", "type" => "RecordingJob", "queue" => "test",
             "priority" => 100, "attempts" => 1, "dequeued_at" => 1001,
             "payload" => { "n" => 2 } }

    stub_request(:get, "#{URL}/jobs/take?prefetch=2")
      .to_return(
        { status: 200,
          body: "#{JSON.generate(job1)}\n#{JSON.generate(job2)}\n",
          headers: { "Content-Type" => "application/x-ndjson" } },
        { status: 200, body: "",
          headers: { "Content-Type" => "application/x-ndjson" } }
      )

    stub_request(:post, %r{#{URL}/jobs/j[12]/success})
      .to_return(status: 204)

    worker = Zanxio::Worker.new(thread_count: 1, fiber_count: 2, prefetch: 2,
                                 logger: Logger.new(File::NULL))

    t = Thread.new { worker.run }

    deadline = Time.now + 5
    sleep 0.05 while RecordingJob.results.size < 2 && Time.now < deadline

    worker.instance_variable_set(:@shutdown, true)
    worker.instance_variable_get(:@dispatch_queue).close rescue nil
    t.join(5)

    # Both jobs were processed across the 2 fibers
    assert_equal 2, RecordingJob.results.size
    ids = RecordingJob.results.map { |r| r[:id] }.sort
    assert_equal %w[j1 j2], ids
  end

  def test_worker_id_proc
    worker = Zanxio::Worker.new(
      worker_id: ->(t, f) { "app-#{Process.pid}-t#{t}-f#{f}" }
    )

    wid = worker.send(:resolve_worker_id, 2, 3)
    assert_equal "app-#{Process.pid}-t2-f3", wid
  end

  def test_worker_id_nil_by_default
    worker = Zanxio::Worker.new
    wid = worker.send(:resolve_worker_id, 0, 0)
    assert_nil wid
  end
end
