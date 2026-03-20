# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# frozen_string_literal: true

# Entrypoint for the profiler script ./bin/profile-worker. Should also
# work with the standard worker too for benchmarking.
#
# This enqueues a largely no-op job a given number of times (default 10,000).
# The worker then starts with those jobs on the queue and processes them until
# it performs the Nth job which is at the back of the queue and then exits the
# process.
#
# Change the environment variable JOB_COUNT to control how many jobs are
# enqueued.

require 'zizq'
require 'async'

Zizq.configure do |c|
  c.url = ENV['ZIZQ_URL'] if ENV['ZIZQ_URL']

  if ENV['ZIZQ_CA']
    c.tls = {
      ca: ENV['ZIZQ_CA'],
      client_cert: ENV['ZIZQ_CLIENT_CERT'],
      client_key: ENV['ZIZQ_CLIENT_KEY'],
    }
  end
end

# As close to a no-op as possible.
class TestJob
  include Zizq::Job

  @@started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  def perform(n, total)
    if n == total
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - @@started_at
      warn format(
        "Processed %d jobs in %.3fs (%.3f jobs/s). Terminating.",
        total,
        elapsed,
        total/elapsed
      )
      Process.kill("TERM", Process.pid)
    end
  end
end

job_count = Integer(ENV.fetch("JOB_COUNT", 10_000))

warn "Enqueueing #{job_count} jobs"

Sync do
  (1..job_count).each_slice(1_000) do |nums|
    Zizq.enqueue_bulk do |b|
      nums.each do |n|
        b.enqueue(TestJob, n, job_count)
      end
    end
  end
end
