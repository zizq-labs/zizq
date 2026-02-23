# Copyright (c) 2026 Chris Corbyn <chris@zanxio.io>
# Licensed under the MIT License. See LICENSE file for details.

# frozen_string_literal: true

require "test_helper"

class TestConfiguration < Minitest::Test
  def setup
    Zanxio.reset!
  end

  def test_defaults
    config = Zanxio::Configuration.new
    assert_equal "http://localhost:7890", config.url
    assert_equal :msgpack, config.format
    assert_instance_of Logger, config.logger
  end

  def test_configure_block
    Zanxio.configure do |c|
      c.url = "http://localhost:7890"
      c.format = :json
    end

    assert_equal "http://localhost:7890", Zanxio.configuration.url
    assert_equal :json, Zanxio.configuration.format
  end

  def test_validate_rejects_empty_url
    config = Zanxio::Configuration.new
    config.url = ""
    assert_raises(ArgumentError) { config.validate! }
  end

  def test_validate_rejects_invalid_format
    config = Zanxio::Configuration.new
    config.url = "http://localhost:7890"
    config.format = :xml
    assert_raises(ArgumentError) { config.validate! }
  end

  def test_validate_accepts_valid_config
    config = Zanxio::Configuration.new
    config.url = "http://localhost:7890"
    config.format = :msgpack
    config.validate! # should not raise
  end

  def test_client_memoized
    Zanxio.configure { |c| c.url = "http://localhost:7890" }
    client1 = Zanxio.client
    client2 = Zanxio.client
    assert_same client1, client2
  end

  def test_reset_clears_client
    Zanxio.configure { |c| c.url = "http://localhost:7890" }
    client1 = Zanxio.client
    Zanxio.reset!
    Zanxio.configure { |c| c.url = "http://localhost:7890" }
    client2 = Zanxio.client
    refute_same client1, client2
  end
end
