# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)

require "zizq"
require "minitest/autorun"
require "webmock/minitest"
require "httpx/adapters/webmock"

# The HTTPX adapter is registered after WebMock.enable! was called,
# so we need to explicitly enable it.
WebMock::HttpLibAdapters::HttpxAdapter.enable!
