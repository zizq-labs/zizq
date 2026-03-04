# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# frozen_string_literal: true

module Zizq
  module Resources
    autoload :Resource,    "zizq/resources/resource"
    autoload :Job,         "zizq/resources/job"
    autoload :ErrorRecord, "zizq/resources/error_record"
    autoload :Page,        "zizq/resources/page"
    autoload :JobPage,     "zizq/resources/job_page"
    autoload :ErrorPage,   "zizq/resources/error_page"
  end
end
