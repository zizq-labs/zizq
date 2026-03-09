# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# frozen_string_literal: true

require "test_helper"

# Test job class with a custom queue.
class SendEmailJob
  include Zizq::Job

  zizq_queue "emails"
  zizq_priority 20

  attr_reader :received_user_id, :received_template

  def perform(user_id, template: "default")
    @received_user_id = user_id
    @received_template = template
  end
end

# Test job class using the default queue.
class DefaultQueueJob
  include Zizq::Job

  attr_reader :received_args, :received_kwargs

  def perform(*args, **kwargs)
    @received_args = args
    @received_kwargs = kwargs
  end
end

# Test job class with retry/backoff configuration.
class RetryConfiguredJob
  include Zizq::Job

  zizq_queue "retries"
  zizq_retry_limit 5
  zizq_backoff exponent: 2.0, base: 1.5, jitter: 0.5

  def perform(*) = nil
end

# Test job class that doesn't implement perform.
class UnimplementedJob
  include Zizq::Job
end

# Test job class with a custom zizq_enqueue_options override.
class PriorityOverrideJob
  include Zizq::Job

  zizq_queue "priority"

  def self.zizq_enqueue_options(*args, **kwargs)
    opts = super
    opts.priority = 0 if args.first == "urgent"
    opts
  end

  def perform(level) = nil
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

  # --- zizq_serialize / zizq_deserialize ---

  def test_serialize_args_and_kwargs
    payload = SendEmailJob.zizq_serialize(42, template: "welcome")
    assert_equal({ "args" => [42], "kwargs" => { "template" => "welcome" } }, payload)
  end

  def test_serialize_no_args
    payload = DefaultQueueJob.zizq_serialize
    assert_equal({ "args" => [], "kwargs" => {} }, payload)
  end

  def test_deserialize_round_trips
    original_args = [1, "two"]
    original_kwargs = { key: "val" }
    payload = DefaultQueueJob.zizq_serialize(*original_args, **original_kwargs)
    args, kwargs = DefaultQueueJob.zizq_deserialize(payload)
    assert_equal original_args, args
    assert_equal original_kwargs, kwargs
  end

  # --- zizq_enqueue_options ---

  def test_enqueue_options_defaults
    opts = SendEmailJob.zizq_enqueue_options(42)
    assert_equal "emails", opts.queue
    assert_equal 20, opts.priority
    assert_nil opts.delay
    assert_nil opts.retry_limit
  end

  def test_enqueue_options_includes_class_config
    opts = RetryConfiguredJob.zizq_enqueue_options
    assert_equal "retries", opts.queue
    assert_equal 5, opts.retry_limit
    assert_equal({ exponent: 2.0, base: 1.5, jitter: 0.5 }, opts.backoff)
  end

  def test_enqueue_options_custom_override
    opts = PriorityOverrideJob.zizq_enqueue_options("urgent")
    assert_equal 0, opts.priority

    opts2 = PriorityOverrideJob.zizq_enqueue_options("normal")
    assert_nil opts2.priority
  end

  # --- perform ---

  def test_perform_receives_args_and_kwargs
    job = SendEmailJob.new
    job.perform(42, template: "welcome")
    assert_equal 42, job.received_user_id
    assert_equal "welcome", job.received_template
  end

  def test_perform_with_defaults
    job = SendEmailJob.new
    job.perform(42)
    assert_equal 42, job.received_user_id
    assert_equal "default", job.received_template
  end

  def test_unimplemented_perform_raises
    job = UnimplementedJob.new
    assert_raises(NotImplementedError) { job.perform }
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

  def test_enqueue_with_args_and_kwargs
    stub_request(:post, "#{URL}/jobs")
      .with { |req|
        body = JSON.parse(req.body)
        body["type"] == "SendEmailJob" &&
          body["queue"] == "emails" &&
          body["payload"] == { "args" => [42], "kwargs" => { "template" => "welcome" } }
      }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    result = Zizq.enqueue(SendEmailJob, 42, template: "welcome")
    assert_equal "x", result.id
  end

  def test_enqueue_uses_class_queue_by_default
    stub_request(:post, "#{URL}/jobs")
      .with { |req| JSON.parse(req.body)["queue"] == "emails" }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(SendEmailJob, 42)
  end

  def test_enqueue_default_queue_fallback
    stub_request(:post, "#{URL}/jobs")
      .with { |req| JSON.parse(req.body)["queue"] == "default" }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(DefaultQueueJob)
  end

  def test_enqueue_queue_override_via_block
    stub_request(:post, "#{URL}/jobs")
      .with { |req| JSON.parse(req.body)["queue"] == "priority" }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(SendEmailJob, 42) { |o| o.queue = "priority" }
  end

  def test_enqueue_with_priority_via_block
    stub_request(:post, "#{URL}/jobs")
      .with { |req| JSON.parse(req.body)["priority"] == 100 }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(SendEmailJob, 42) { |o| o.priority = 100 }
  end

  def test_enqueue_with_ready_at_via_block
    now = Time.now
    expected_ready_at = ((now.to_f + 60) * 1000).to_i

    stub_request(:post, "#{URL}/jobs")
      .with { |req|
        body = JSON.parse(req.body)
        body["ready_at"] == expected_ready_at
      }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(SendEmailJob, 42) { |o| o.ready_at = now + 60 }
  end

  def test_enqueue_with_delay_via_block
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

    Zizq.enqueue(SendEmailJob, 42) { |o| o.delay = 60 }
  end

  def test_enqueue_uses_class_retry_limit
    stub_request(:post, "#{URL}/jobs")
      .with { |req|
        body = JSON.parse(req.body)
        body["retry_limit"] == 5
      }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(RetryConfiguredJob)
  end

  def test_enqueue_uses_class_backoff_converted_to_ms
    stub_request(:post, "#{URL}/jobs")
      .with { |req|
        body = JSON.parse(req.body)
        # 1.5s -> 1500ms, 0.5s -> 500ms
        body["backoff"] == { "exponent" => 2.0, "base_ms" => 1500.0, "jitter_ms" => 500.0 }
      }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(RetryConfiguredJob)
  end

  def test_enqueue_block_overrides_class_retry_limit
    stub_request(:post, "#{URL}/jobs")
      .with { |req| JSON.parse(req.body)["retry_limit"] == 10 }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(RetryConfiguredJob) { |o| o.retry_limit = 10 }
  end

  def test_enqueue_with_custom_enqueue_options_override
    stub_request(:post, "#{URL}/jobs")
      .with { |req|
        body = JSON.parse(req.body)
        body["priority"] == 0 && body["queue"] == "priority"
      }
      .to_return(status: 201, body: JSON.generate({ "id" => "x" }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue(PriorityOverrideJob, "urgent")
  end

  def test_enqueue_rejects_class_without_job_mixin
    assert_raises(ArgumentError) { Zizq.enqueue(String) }
  end

  def test_enqueue_anonymous_class_raises
    klass = Class.new { include Zizq::Job }
    assert_raises(ArgumentError) { Zizq.enqueue(klass) }
  end

  # --- Zizq.enqueue_bulk ---

  def test_enqueue_bulk_collects_and_sends_single_request
    jobs_response = { "jobs" => [
      { "id" => "j1", "type" => "SendEmailJob", "queue" => "emails", "status" => "ready" },
      { "id" => "j2", "type" => "DefaultQueueJob", "queue" => "default", "status" => "ready" }
    ] }

    stub_request(:post, "#{URL}/jobs/bulk")
      .with { |req|
        body = JSON.parse(req.body)
        body["jobs"].size == 2 &&
          body["jobs"][0]["type"] == "SendEmailJob" &&
          body["jobs"][0]["queue"] == "emails" &&
          body["jobs"][0]["payload"] == { "args" => [42], "kwargs" => { "template" => "welcome" } } &&
          body["jobs"][1]["type"] == "DefaultQueueJob" &&
          body["jobs"][1]["queue"] == "default"
      }
      .to_return(status: 201, body: JSON.generate(jobs_response),
                 headers: { "Content-Type" => "application/json" })

    result = Zizq.enqueue_bulk do |b|
      b.enqueue(SendEmailJob, 42, template: "welcome")
      b.enqueue(DefaultQueueJob)
    end

    assert_instance_of Array, result
    assert_equal 2, result.size
    assert_equal "j1", result[0].id
    assert_equal "j2", result[1].id
  end

  def test_enqueue_bulk_supports_option_overrides
    stub_request(:post, "#{URL}/jobs/bulk")
      .with { |req|
        body = JSON.parse(req.body)
        body["jobs"][0]["queue"] == "priority" &&
          body["jobs"][0]["priority"] == 100
      }
      .to_return(status: 201, body: JSON.generate({ "jobs" => [{ "id" => "j1" }] }),
                 headers: { "Content-Type" => "application/json" })

    Zizq.enqueue_bulk do |b|
      b.enqueue(SendEmailJob, 42) { |o| o.queue = "priority"; o.priority = 100 }
    end
  end

  def test_enqueue_bulk_empty_returns_empty_array
    result = Zizq.enqueue_bulk { |_b| }
    assert_equal [], result
  end

  def test_enqueue_bulk_rejects_non_job_class
    assert_raises(ArgumentError) do
      Zizq.enqueue_bulk { |b| b.enqueue(String) }
    end
  end
end
