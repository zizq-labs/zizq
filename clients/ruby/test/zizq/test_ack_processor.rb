# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# frozen_string_literal: true

require "test_helper"

class TestAckProcessor < Minitest::Test
  URL = "http://localhost:7890"

  def setup
    Zizq.reset!
    Zizq.configure { |c| c.url = URL; c.format = :json }
  end

  def new_processor(capacity: 10)
    Zizq::AckProcessor.new(
      client: Zizq.client,
      capacity: capacity,
      logger: Logger.new(File::NULL),
      backoff: Zizq::Backoff.new(min_wait: 0.1, max_wait: 5.0, multiplier: 2.0)
    )
  end

  def test_single_ack
    stub = stub_request(:post, "#{URL}/jobs/success")
      .with { |req| JSON.parse(req.body)["ids"] == ["j1"] }
      .to_return(status: 204)

    proc = new_processor
    proc.start
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j1"))
    proc.stop(timeout: 5)

    assert_requested(stub, times: 1)
  end

  def test_single_nack
    stub = stub_request(:post, "#{URL}/jobs/j1/failure")
      .with { |req|
        body = JSON.parse(req.body)
        body["message"] == "RuntimeError: boom" &&
          body["error_type"] == "RuntimeError" &&
          body["backtrace"] == "line1\nline2"
      }
      .to_return(status: 200, body: JSON.generate({ "id" => "j1", "status" => "scheduled" }),
                 headers: { "Content-Type" => "application/json" })

    proc = new_processor
    proc.start
    proc.push(Zizq::AckProcessor::Nack.new(
      job_id: "j1",
      message: "RuntimeError: boom",
      error_type: "RuntimeError",
      backtrace: "line1\nline2"
    ))
    proc.stop(timeout: 5)

    assert_requested(stub, times: 1)
  end

  def test_batch_of_mixed_acks_and_nacks
    bulk_stub = stub_request(:post, "#{URL}/jobs/success")
      .to_return(status: 204)
    nack_stub = stub_request(:post, "#{URL}/jobs/j2/failure")
      .to_return(status: 200, body: JSON.generate({ "id" => "j2" }),
                 headers: { "Content-Type" => "application/json" })
    individual_ack_stub = stub_request(:post, %r{#{URL}/jobs/j[13]/success})
      .to_return(status: 204)

    proc = new_processor
    proc.start
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j1"))
    proc.push(Zizq::AckProcessor::Nack.new(
      job_id: "j2", message: "err", error_type: "E", backtrace: nil
    ))
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j3"))
    proc.stop(timeout: 5)

    assert_requested(bulk_stub, at_least_times: 1)
    assert_requested(nack_stub, times: 1)
    assert_not_requested(individual_ack_stub)
  end

  def test_retry_on_500
    stub = stub_request(:post, "#{URL}/jobs/success")
      .to_return({ status: 500, body: JSON.generate({ "error" => "internal" }),
                   headers: { "Content-Type" => "application/json" } },
                 { status: 204 })

    proc = new_processor
    proc.start
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j1"))
    # Backoff for first retry is 0.2s; wait long enough for it to complete
    sleep 0.5
    proc.stop(timeout: 5)

    assert_requested(stub, times: 2)
  end

  def test_drop_on_422
    stub = stub_request(:post, "#{URL}/jobs/success")
      .to_return(status: 422, body: JSON.generate({ "not_found" => ["j1"] }),
                 headers: { "Content-Type" => "application/json" })

    proc = new_processor
    proc.start
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j1"))
    proc.stop(timeout: 5)

    # 422 is silently accepted — no retry
    assert_requested(stub, times: 1)
  end

  def test_drop_on_4xx
    stub = stub_request(:post, "#{URL}/jobs/success")
      .to_return(status: 400, body: JSON.generate({ "error" => "bad request" }),
                 headers: { "Content-Type" => "application/json" })

    proc = new_processor
    proc.start
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j1"))
    proc.stop(timeout: 5)

    # 4xx is dropped — no retry
    assert_requested(stub, times: 1)
  end

  def test_retries_do_not_block_fresh_acks
    stub = stub_request(:post, "#{URL}/jobs/success")
      .to_return({ status: 500, body: JSON.generate({ "error" => "internal" }),
                   headers: { "Content-Type" => "application/json" } },
                 { status: 204 })

    proc = new_processor
    proc.start
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j1"))
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j2"))
    # Wait for retry to complete (backoff 0.2s)
    sleep 0.5
    proc.stop(timeout: 5)

    assert_requested(stub, times: 2)
  end

  def test_clean_shutdown_drains_queue
    stub = stub_request(:post, "#{URL}/jobs/success")
      .to_return(status: 204)

    proc = new_processor
    proc.start
    5.times { |i| proc.push(Zizq::AckProcessor::Ack.new(job_id: "j#{i + 1}")) }
    proc.stop(timeout: 5)

    # All 5 IDs should have been sent via bulk endpoint (1 or more calls)
    assert_requested(stub, at_least_times: 1)
  end
end
