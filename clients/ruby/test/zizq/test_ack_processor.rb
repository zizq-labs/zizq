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
    stub = stub_request(:post, "#{URL}/jobs/j1/success")
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
        body["error"] == "RuntimeError: boom" &&
          body["error_type"] == "RuntimeError" &&
          body["backtrace"] == "line1\nline2"
      }
      .to_return(status: 200, body: JSON.generate({ "id" => "j1", "status" => "scheduled" }),
                 headers: { "Content-Type" => "application/json" })

    proc = new_processor
    proc.start
    proc.push(Zizq::AckProcessor::Nack.new(
      job_id: "j1",
      error: "RuntimeError: boom",
      error_type: "RuntimeError",
      backtrace: "line1\nline2"
    ))
    proc.stop(timeout: 5)

    assert_requested(stub, times: 1)
  end

  def test_batch_of_mixed_acks_and_nacks
    ack_stub1 = stub_request(:post, "#{URL}/jobs/j1/success")
      .to_return(status: 204)
    nack_stub = stub_request(:post, "#{URL}/jobs/j2/failure")
      .to_return(status: 200, body: JSON.generate({ "id" => "j2" }),
                 headers: { "Content-Type" => "application/json" })
    ack_stub2 = stub_request(:post, "#{URL}/jobs/j3/success")
      .to_return(status: 204)

    proc = new_processor
    proc.start
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j1"))
    proc.push(Zizq::AckProcessor::Nack.new(
      job_id: "j2", error: "err", error_type: "E", backtrace: nil
    ))
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j3"))
    proc.stop(timeout: 5)

    assert_requested(ack_stub1, times: 1)
    assert_requested(nack_stub, times: 1)
    assert_requested(ack_stub2, times: 1)
  end

  def test_retry_on_500
    stub = stub_request(:post, "#{URL}/jobs/j1/success")
      .to_return({ status: 500 }, { status: 204 })

    proc = new_processor
    proc.start
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j1"))
    # Backoff for first retry is 0.2s; wait long enough for it to complete
    sleep 0.5
    proc.stop(timeout: 5)

    assert_requested(stub, times: 2)
  end

  def test_drop_on_404
    stub = stub_request(:post, "#{URL}/jobs/j1/success")
      .to_return(status: 404, body: JSON.generate({ "error" => "not found" }),
                 headers: { "Content-Type" => "application/json" })

    proc = new_processor
    proc.start
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j1"))
    proc.stop(timeout: 5)

    # Should only be called once — no retry
    assert_requested(stub, times: 1)
  end

  def test_drop_on_4xx
    stub = stub_request(:post, "#{URL}/jobs/j1/success")
      .to_return(status: 422, body: JSON.generate({ "error" => "unprocessable" }),
                 headers: { "Content-Type" => "application/json" })

    proc = new_processor
    proc.start
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j1"))
    proc.stop(timeout: 5)

    # Should only be called once — no retry
    assert_requested(stub, times: 1)
  end

  def test_retries_do_not_block_fresh_acks
    j1_stub = stub_request(:post, "#{URL}/jobs/j1/success")
      .to_return({ status: 500 }, { status: 204 })
    j2_stub = stub_request(:post, "#{URL}/jobs/j2/success")
      .to_return(status: 204)

    proc = new_processor
    proc.start
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j1"))
    proc.push(Zizq::AckProcessor::Ack.new(job_id: "j2"))
    # Wait for j1's retry to complete (backoff 0.2s)
    sleep 0.5
    proc.stop(timeout: 5)

    # j2 succeeded immediately; j1 was retried once
    assert_requested(j1_stub, times: 2)
    assert_requested(j2_stub, times: 1)
  end

  def test_clean_shutdown_drains_queue
    stubs = (1..5).map do |i|
      stub_request(:post, "#{URL}/jobs/j#{i}/success")
        .to_return(status: 204)
    end

    proc = new_processor
    proc.start
    5.times { |i| proc.push(Zizq::AckProcessor::Ack.new(job_id: "j#{i + 1}")) }
    proc.stop(timeout: 5)

    stubs.each { |s| assert_requested(s, times: 1) }
  end
end
