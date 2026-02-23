# Copyright (c) 2026 Chris Corbyn <chris@zanxio.io>
# Licensed under the MIT License. See LICENSE file for details.

# frozen_string_literal: true

module Zanxio
  module Resources
    autoload :Resource,    "zanxio/resources/resource"
    autoload :Job,         "zanxio/resources/job"
    autoload :ErrorRecord, "zanxio/resources/error_record"
    autoload :Page,        "zanxio/resources/page"
    autoload :JobPage,     "zanxio/resources/job_page"
    autoload :ErrorPage,   "zanxio/resources/error_page"
  end
end
