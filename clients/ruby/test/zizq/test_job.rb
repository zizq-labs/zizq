# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# frozen_string_literal: true

require "test_helper"

# Test job class with a custom queue.
class SendEmailJob
  include Zizq::Job

  zizq_queue "emails"

  attr_reader :received_payload

  def perform(payload)
    @received_payload = payload
  end
end

# Test job class using the default queue.
class DefaultQueueJob
  include Zizq::Job

  attr_reader :received_payload

  def perform(payload)
    @received_payload = payload
  end
end

# Test job class with retry/backoff configuration.
class RetryConfiguredJob
  include Zizq::Job

  zizq_queue "retries"
  zizq_retry_limit 5
  zizq_backoff exponent: 2.0, base: 1.5, jitter: 0.5

  def perform(_payload) = nil
end

# Test job class that doesn't implement perform.
class UnimplementedJob
  include Zizq::Job
end

class TestJob < Minitest::Test
  URL = "http://localhost:7890"

  def setup
    Zizq.reset!
    Zizq.configure { |c| c.url = URL; c.format = :json }
  end

  # --- zizq_queue class method ---

  def test_custom_queue
    assert_equal "emails", SendEmailJob.zizq_queue
  end

  def test_default_queue
    assert_equal "default", DefaultQueueJob.zizq_queue
  end

  # --- zizq_retry_limit class method ---

  def test_retry_limit_configured
    assert_equal 5, RetryConfiguredJob.zizq_retry_limit
  end

  def test_retry_limit_nil_by_default
    assert_nil DefaultQueueJob.zizq_retry_limit
  end

  # --- zizq_backoff class method ---

  def test_backoff_configured
    expected = { exponent: 2.0, base: 1.5, jitter: 0.5 }
    assert_equal expected, RetryConfiguredJob.zizq_backoff
  end

  def test_backoff_nil_by_default
    assert_nil DefaultQueueJob.zizq_backoff
  end

  def test_backoff_requires_all_args
    klass = Class.new { include Zizq::Job }
    assert_raises(ArgumentError) { klass.zizq_backoff(exponent: 2.0) }
    assert_raises(ArgumentError) { klass.zizq_backoff(base: 1.0) }
  end

  # --- perform ---

  def test_perform_receives_payload
    job = SendEmailJob.new
    job.perform({ "user_id" => 42 })
    assert_equal({ "user_id" => 42 }, job.received_payload)
  end

  def test_unimplemented_perform_raises
    job = UnimplementedJob.new
    assert_raises(NotImplementedError) { job.perform({}) }
  end

  # --- metadata helpers ---

  def test_metadata_helpers
    client = Zizq::Client.new(url: URL, format: :json)
    resource_job = Zizq::Resources::Job.new(client, {
      "id" => "job-123",
      "attempts" => 3,
      "queue" => "emails",
      "priority" => 100,
      "dequeued_at" => 1_700_000_000_000
    })

    job = SendEmailJob.new
    job.set_zizq_job(resource_job)

    assert_equal "job-123", job.zizq_id
    assert_equal 3, job.zizq_attempts
    assert_equal "emails", job.zizq_queue
    assert_equal 100, job.zizq_priority
    assert_in_delta 1_700_000_000.0, job.zizq_dequeued_at, 0.001
  end

  def test_metadata_nil_before_set
    job = SendEmailJob.new
    assert_nil job.zizq_id
    assert_nil job.zizq_attempts
  end

  # --- Zizq.enqueue ---

  def test_enqueue_with_class
    stub_request(:post, "#{URL}/jobs")
      .with { |req|
        body = JSON.parse(req.body)
        body["type"] == "SendEmailJob" &&
          body["queue"] == "emails" &&
          body["payload"] == { "user_id" => 42 }
      }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    result = Zizq.enqueue(SendEmailJob, { user_id: 42 })
    assert_equal "x", result.id
  end

  def test_enqueue_uses_class_queue_by_default
    stub_request(:post, "#{URL}/jobs")
      .with { |req| JSON.parse(req.body)["queue"] == "emails" }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(SendEmailJob, {})
  end

  def test_enqueue_default_queue_fallback
    stub_request(:post, "#{URL}/jobs")
      .with { |req| JSON.parse(req.body)["queue"] == "default" }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(DefaultQueueJob, {})
  end

  def test_enqueue_queue_override
    stub_request(:post, "#{URL}/jobs")
      .with { |req| JSON.parse(req.body)["queue"] == "priority" }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(SendEmailJob, {}, queue: "priority")
  end

  def test_enqueue_with_priority
    stub_request(:post, "#{URL}/jobs")
      .with { |req| JSON.parse(req.body)["priority"] == 100 }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(SendEmailJob, {}, priority: 100)
  end

  def test_enqueue_with_delay
    # Freeze time for predictable ready_at
    now = Time.now.to_f
    expected_ready_at = ((now + 60) * 1000).to_i

    stub_request(:post, "#{URL}/jobs")
      .with { |req|
        body = JSON.parse(req.body)
        # Allow 1 second of drift for test execution time
        (body["ready_at"] - expected_ready_at).abs < 1000
      }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(SendEmailJob, {}, delay: 60)
  end

  def test_enqueue_uses_class_retry_limit
    stub_request(:post, "#{URL}/jobs")
      .with { |req|
        body = JSON.parse(req.body)
        body["retry_limit"] == 5
      }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(RetryConfiguredJob, {})
  end

  def test_enqueue_uses_class_backoff_converted_to_ms
    stub_request(:post, "#{URL}/jobs")
      .with { |req|
        body = JSON.parse(req.body)
        # 1.5s → 1500ms, 0.5s → 500ms
        body["backoff"] == { "exponent" => 2.0, "base_ms" => 1500.0, "jitter_ms" => 500.0 }
      }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(RetryConfiguredJob, {})
  end

  def test_enqueue_kwarg_overrides_class_retry_limit
    stub_request(:post, "#{URL}/jobs")
      .with { |req| JSON.parse(req.body)["retry_limit"] == 10 }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(RetryConfiguredJob, {}, retry_limit: 10)
  end

  def test_enqueue_rejects_class_without_job_mixin
    assert_raises(ArgumentError) { Zizq.enqueue(String, {}) }
  end

  def test_enqueue_anonymous_class_raises
    klass = Class.new { include Zizq::Job }
    assert_raises(ArgumentError) { Zizq.enqueue(klass, {}) }
  end
end
