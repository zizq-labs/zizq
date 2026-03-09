# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# frozen_string_literal: true

# Entrypoint for running a simulation script via ./bin/zizq-worker.
#
# This enqueues a given number of randomised jobs, each of which simulates
# some latency-bound operation, occasionally fails, and simulates enqueueing
# more work so that the queue remains busy.
#
# Change the environment variable JOB_COUNT to control how many jobs are
# initially enqueued.

require 'zizq'
require 'async'

class SimulationBaseJob
  include Zizq::Job

  def perform
    process_for(self.class)
  end
end

class SendEmailJob < SimulationBaseJob
  zizq_queue 'comms'
  zizq_priority 50
end

class GenerateReportJob < SimulationBaseJob
  zizq_queue 'analytics'
  zizq_priority 1000
end

class ProcessVideoJob < SimulationBaseJob
  zizq_priority 500
end

class FulfillOrderJob < SimulationBaseJob
  zizq_queue 'payments'
  zizq_priority 40
end

class ClearNotesJob < SimulationBaseJob
  zizq_priority 1000
end

JOB_CHOICES = [
  *([SendEmailJob] * 40),
  *([FulfillOrderJob] * 10),
  *([ProcessVideoJob] * 3),
  *([GenerateReportJob] * 2),
  *([ClearNotesJob] * 1),
]

SCHEDULE_PROBABILITY = {
  SendEmailJob => 0.2,
  FulfillOrderJob => 0.0,
  ProcessVideoJob => 0.0,
  GenerateReportJob => 1.0,
  ClearNotesJob => 1.0,
}

SCHEDULE_WAIT = {
  SendEmailJob => (5.0..30.0),
  FulfillOrderJob => (0.0..0.0),
  ProcessVideoJob => (0.0..0.0),
  GenerateReportJob => (30.0..30.00),
  ClearNotesJob => (10.0..10.0),
}

ERROR_PROBABILITY = {
  SendEmailJob => 0.0001,
  FulfillOrderJob => 0.001,
  ProcessVideoJob => 0.01,
  GenerateReportJob => 0.0001,
  ClearNotesJob => 0.0,
}

JOB_LATENCY = {
  SendEmailJob => (0.1..1.0),
  FulfillOrderJob => (2.0..5.0),
  ProcessVideoJob => (10.0..60.0),
  GenerateReportJob => (120.0..180.00),
  ClearNotesJob => (10.0..30.0),
}

def enqueue_random_job(z)
  job_class = JOB_CHOICES.sample

  z.enqueue(job_class) do |o|
    if rand() < SCHEDULE_PROBABILITY.fetch(job_class)
      o.ready_at = Time.now + rand(SCHEDULE_WAIT.fetch(job_class))
    end
  end
end

def process_for(job_class)
  sleep rand(JOB_LATENCY.fetch(job_class))

  if rand() < ERROR_PROBABILITY.fetch(job_class)
    raise 'Simulated error'
  end

  enqueue_random_job(Zizq)
end

job_count = Integer(ENV.fetch("JOB_COUNT", 5_000))

warn "Enqueueing #{job_count} jobs"

Sync do
  (1..job_count).each_slice(1_000) do |nums|
    Zizq.enqueue_bulk do |b|
      nums.each { enqueue_random_job(b) }
    end
  end
end
