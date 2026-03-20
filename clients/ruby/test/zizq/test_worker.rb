# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# frozen_string_literal: true

require "test_helper"

# A simple job that records what it received.
class RecordingJob
  include Zizq::Job

  zizq_queue "test"

  # Class-level storage so we can inspect results from the test.
  @results = []

  class << self
    attr_accessor :results
  end

  def perform(user_id, action: nil)
    self.class.results << {
      user_id: user_id,
      action: action,
      id: zizq_id,
      attempts: zizq_attempts,
      queue: zizq_queue,
      priority: zizq_priority
    }
  end
end

# A job that raises on perform.
class FailingJob
  include Zizq::Job

  @fail_count = 0

  class << self
    attr_accessor :fail_count
  end

  def perform(*)
    self.class.fail_count += 1
    raise RuntimeError, "boom"
  end
end

class TestWorker < Minitest::Test
  URL = "http://localhost:7890"

  def setup
    Zizq.reset!
    Zizq.configure { |c| c.url = URL; c.format = :json }
    RecordingJob.results = []
    FailingJob.fail_count = 0
  end

  def test_defaults
    worker = Zizq::Worker.new
    assert_equal 5, worker.thread_count
    assert_equal 1, worker.fiber_count
    assert_equal 10, worker.prefetch
    assert_equal [], worker.queues
    assert_equal 30, worker.shutdown_timeout
    assert_equal 1, worker.backoff.min_wait
    assert_equal 30, worker.backoff.max_wait
  end

  def test_custom_prefetch
    worker = Zizq::Worker.new(thread_count: 4, fiber_count: 2, prefetch: 10)
    assert_equal 10, worker.prefetch
  end

  def test_default_prefetch_is_threads_times_fibers
    worker = Zizq::Worker.new(thread_count: 4, fiber_count: 3)
    assert_equal 24, worker.prefetch
  end

  def test_dispatches_job_successfully
    payload = { "args" => [42], "kwargs" => { "action" => "signup" } }
    job1 = { "id" => "j1", "type" => "RecordingJob", "queue" => "test",
             "priority" => 100, "attempts" => 1, "dequeued_at" => 1000,
             "payload" => payload }

    stub_request(:get, "#{URL}/jobs/take?prefetch=1")
      .to_return(
        { status: 200, body: "#{JSON.generate(job1)}\n",
          headers: { "Content-Type" => "application/x-ndjson" } },
        { status: 200, body: "",
          headers: { "Content-Type" => "application/x-ndjson" } }
      )

    ack_stub = stub_request(:post, "#{URL}/jobs/success")
      .to_return(status: 204)

    worker = Zizq::Worker.new(thread_count: 1, queues: [], prefetch: 1,
                                 shutdown_timeout: 2, logger: Logger.new(File::NULL))

    t = Thread.new { worker.run }

    deadline = Time.now + 5
    sleep 0.05 while RecordingJob.results.empty? && Time.now < deadline

    worker.shutdown
    t.join(5)

    assert_equal 1, RecordingJob.results.size
    result = RecordingJob.results.first
    assert_equal 42, result[:user_id]
    assert_equal "signup", result[:action]
    assert_equal "j1", result[:id]
    assert_equal 1, result[:attempts]
    assert_equal "test", result[:queue]
    assert_equal 100, result[:priority]

    assert_requested(ack_stub, at_least_times: 1)
  end

  def test_nacks_failing_job
    payload = { "args" => [], "kwargs" => {} }
    job1 = { "id" => "j1", "type" => "FailingJob", "queue" => "default",
             "priority" => 32_768, "attempts" => 1, "payload" => payload }

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
        body["message"].include?("RuntimeError") &&
          body["message"].include?("boom") &&
          body["error_type"] == "RuntimeError"
      }
      .to_return(status: 200, body: JSON.generate({ "id" => "j1", "status" => "scheduled" }),
                 headers: { "Content-Type" => "application/json" })

    worker = Zizq::Worker.new(thread_count: 1, prefetch: 1,
                                 shutdown_timeout: 2, logger: Logger.new(File::NULL))

    t = Thread.new { worker.run }

    deadline = Time.now + 5
    sleep 0.05 while FailingJob.fail_count == 0 && Time.now < deadline

    worker.shutdown
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

    # Should nack without kill -- just a normal failure so backoff retry kicks in
    nack_stub = stub_request(:post, "#{URL}/jobs/j1/failure")
      .with { |req|
        body = JSON.parse(req.body)
        body["error_type"] == "NameError" && !body.key?("kill")
      }
      .to_return(status: 200, body: JSON.generate({ "id" => "j1", "status" => "scheduled" }),
                 headers: { "Content-Type" => "application/json" })

    worker = Zizq::Worker.new(thread_count: 1, prefetch: 1,
                                 shutdown_timeout: 2, logger: Logger.new(File::NULL))

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

    worker.shutdown
    t.join(5)

    assert_requested(nack_stub, at_least_times: 1)
  end

  def test_dispatches_job_with_fibers
    payload = { "args" => [42], "kwargs" => { "action" => "signup" } }
    job1 = { "id" => "j1", "type" => "RecordingJob", "queue" => "test",
             "priority" => 100, "attempts" => 1, "dequeued_at" => 1000,
             "payload" => payload }

    stub_request(:get, "#{URL}/jobs/take?prefetch=2")
      .to_return(
        { status: 200, body: "#{JSON.generate(job1)}\n",
          headers: { "Content-Type" => "application/x-ndjson" } },
        { status: 200, body: "",
          headers: { "Content-Type" => "application/x-ndjson" } }
      )

    ack_stub = stub_request(:post, "#{URL}/jobs/success")
      .to_return(status: 204)

    # 1 thread with 2 fibers exercises the Async code path
    worker = Zizq::Worker.new(thread_count: 1, fiber_count: 2, prefetch: 2,
                                 shutdown_timeout: 2, logger: Logger.new(File::NULL))

    t = Thread.new { worker.run }

    deadline = Time.now + 5
    sleep 0.05 while RecordingJob.results.empty? && Time.now < deadline

    worker.shutdown
    t.join(5)

    assert_equal 1, RecordingJob.results.size
    result = RecordingJob.results.first
    assert_equal 42, result[:user_id]
    assert_equal "signup", result[:action]
    assert_equal "j1", result[:id]
    assert_requested(ack_stub, at_least_times: 1)
  end

  def test_nacks_failing_job_with_fibers
    payload = { "args" => [], "kwargs" => {} }
    job1 = { "id" => "j1", "type" => "FailingJob", "queue" => "default",
             "priority" => 32_768, "attempts" => 1, "payload" => payload }

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
        body["message"].include?("RuntimeError") &&
          body["message"].include?("boom")
      }
      .to_return(status: 200, body: JSON.generate({ "id" => "j1", "status" => "scheduled" }),
                 headers: { "Content-Type" => "application/json" })

    worker = Zizq::Worker.new(thread_count: 1, fiber_count: 2, prefetch: 2,
                                 shutdown_timeout: 2, logger: Logger.new(File::NULL))

    t = Thread.new { worker.run }

    deadline = Time.now + 5
    sleep 0.05 while FailingJob.fail_count == 0 && Time.now < deadline

    worker.shutdown
    t.join(5)

    assert_equal 1, FailingJob.fail_count
    assert_requested(nack_stub, at_least_times: 1)
  end

  def test_multiple_fibers_process_concurrently
    payload1 = { "args" => [1], "kwargs" => {} }
    payload2 = { "args" => [2], "kwargs" => {} }
    job1 = { "id" => "j1", "type" => "RecordingJob", "queue" => "test",
             "priority" => 100, "attempts" => 1, "dequeued_at" => 1000,
             "payload" => payload1 }
    job2 = { "id" => "j2", "type" => "RecordingJob", "queue" => "test",
             "priority" => 100, "attempts" => 1, "dequeued_at" => 1001,
             "payload" => payload2 }

    stub_request(:get, "#{URL}/jobs/take?prefetch=2")
      .to_return(
        { status: 200,
          body: "#{JSON.generate(job1)}\n#{JSON.generate(job2)}\n",
          headers: { "Content-Type" => "application/x-ndjson" } },
        { status: 200, body: "",
          headers: { "Content-Type" => "application/x-ndjson" } }
      )

    stub_request(:post, "#{URL}/jobs/success")
      .to_return(status: 204)

    worker = Zizq::Worker.new(thread_count: 1, fiber_count: 2, prefetch: 2,
                                 shutdown_timeout: 2, logger: Logger.new(File::NULL))

    t = Thread.new { worker.run }

    deadline = Time.now + 5
    sleep 0.05 while RecordingJob.results.size < 2 && Time.now < deadline

    worker.shutdown
    t.join(5)

    # Both jobs were processed across the 2 fibers
    assert_equal 2, RecordingJob.results.size
    ids = RecordingJob.results.map { |r| r[:id] }.sort
    assert_equal %w[j1 j2], ids
  end

  def test_worker_id_proc
    worker = Zizq::Worker.new(
      worker_id: ->(t, f) { "app-#{Process.pid}-t#{t}-f#{f}" }
    )

    wid = worker.send(:resolve_worker_id, 2, 3)
    assert_equal "app-#{Process.pid}-t2-f3", wid
  end

  def test_worker_id_nil_by_default
    worker = Zizq::Worker.new
    wid = worker.send(:resolve_worker_id, 0, 0)
    assert_nil wid
  end
end
