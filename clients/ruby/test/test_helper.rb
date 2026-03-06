# Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
# Licensed under the MIT License. See LICENSE file for details.

# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)

require "zizq"
require "minitest/autorun"
require "webmock/minitest"
require "async/http"
require "webmock/http_lib_adapters/async_http_client_adapter"

WebMock::HttpLibAdapters::AsyncHttpClientAdapter.enable!
