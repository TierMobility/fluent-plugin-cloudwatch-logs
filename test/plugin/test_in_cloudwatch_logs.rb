require 'test_helper'
require 'fluent/test/driver/input'
require 'fluent/test/helpers'
require 'date'
require 'fluent/plugin/in_cloudwatch_logs'

class CloudwatchLogsInputTest < Test::Unit::TestCase
  include CloudwatchLogsTestHelper
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup
  end

  sub_test_case "configure" do
    def test_configure
      d = create_driver(<<-EOC)
        @type cloudwatch_logs
        aws_key_id test_id
        aws_sec_key test_key
        region us-east-1
        tag test
        log_group_name group
        log_stream_name stream
        use_log_stream_name_prefix true
        state_file /tmp/state
      EOC

      assert_equal('test_id', d.instance.aws_key_id)
      assert_equal('test_key', d.instance.aws_sec_key)
      assert_equal('us-east-1', d.instance.region)
      assert_equal('test', d.instance.tag)
      assert_equal('group', d.instance.log_group_name)
      assert_equal('stream', d.instance.log_stream_name)
      assert_equal(true, d.instance.use_log_stream_name_prefix)
      assert_equal('/tmp/state', d.instance.state_file)
      assert_equal(:yajl, d.instance.json_handler)
    end
  end

  sub_test_case "real world" do
    def teardown
      clear_log_group
    end

    def test_emit
      create_log_stream

      time_ms = (Time.now.to_f * 1000).floor
      put_log_events([
        {timestamp: time_ms, message: '{"cloudwatch":"logs1"}'},
        {timestamp: time_ms, message: '{"cloudwatch":"logs2"}'},
      ])

      sleep 5

      d = create_driver
      d.run(expect_emits: 2, timeout: 5)

      emits = d.events
      assert_equal(2, emits.size)
      assert_equal(['test', (time_ms / 1000).floor, {'cloudwatch' => 'logs1'}], emits[0])
      assert_equal(['test', (time_ms / 1000).floor, {'cloudwatch' => 'logs2'}], emits[1])
    end

    def test_emit_width_format
      create_log_stream

      time_ms = (Time.now.to_f * 1000).floor
      put_log_events([
        {timestamp: time_ms, message: 'logs1'},
        {timestamp: time_ms, message: 'logs2'},
      ])

      sleep 5

      d = create_driver(<<-EOC)
      tag test
      @type cloudwatch_logs
      log_group_name #{log_group_name}
      log_stream_name #{log_stream_name}
      state_file /tmp/state
      format /^(?<cloudwatch>[^ ]*)?/
      #{aws_key_id}
      #{aws_sec_key}
      #{region}
      #{endpoint}
    EOC

      d.run(expect_emits: 2, timeout: 5)

      emits = d.events
      assert_equal(2, emits.size)
      assert_equal('test', emits[0][0])
      assert_in_delta((time_ms / 1000).floor, emits[0][1], 10)
      assert_equal({'cloudwatch' => 'logs1'}, emits[0][2])
      assert_equal('test', emits[1][0])
      assert_in_delta((time_ms / 1000).floor, emits[1][1], 10)
      assert_equal({'cloudwatch' => 'logs2'}, emits[1][2])
    end

    def test_emit_with_prefix
      new_log_stream("testprefix")
      create_log_stream

      time_ms = (Time.now.to_f * 1000).floor
      put_log_events([
        {timestamp: time_ms + 1000, message: '{"cloudwatch":"logs1"}'},
        {timestamp: time_ms + 2000, message: '{"cloudwatch":"logs2"}'},
      ])

      new_log_stream("testprefix")
      create_log_stream
      put_log_events([
        {timestamp: time_ms + 3000, message: '{"cloudwatch":"logs3"}'},
        {timestamp: time_ms + 4000, message: '{"cloudwatch":"logs4"}'},
      ])

      sleep 5

      d = create_driver(<<-EOC)
      tag test
      @type cloudwatch_logs
      log_group_name #{log_group_name}
      log_stream_name testprefix
      use_log_stream_name_prefix true
      state_file /tmp/state
      #{aws_key_id}
      #{aws_sec_key}
      #{region}
      #{endpoint}
    EOC
      d.run(expect_emits: 4, timeout: 5)

      emits = d.events
      assert_equal(4, emits.size)
      assert_true(emits.include? ['test', ((time_ms + 1000) / 1000).floor, {'cloudwatch' => 'logs1'}])
      assert_true(emits.include? ['test', ((time_ms + 2000) / 1000).floor, {'cloudwatch' => 'logs2'}])
      assert_true(emits.include? ['test', ((time_ms + 3000) / 1000).floor, {'cloudwatch' => 'logs3'}])
      assert_true(emits.include? ['test', ((time_ms + 4000) / 1000).floor, {'cloudwatch' => 'logs4'}])
    end

    def test_emit_with_todays_log_stream
      new_log_stream("testprefix")
      create_log_stream

      today = DateTime.now.strftime("%Y/%m/%d")
      yesterday = (Date.today - 1).strftime("%Y/%m/%d")
      tomorrow = (Date.today + 1).strftime("%Y/%m/%d")


      time_ms = (Time.now.to_f * 1000).floor
      put_log_events([
        {timestamp: time_ms + 1000, message: '{"cloudwatch":"logs1"}'},
        {timestamp: time_ms + 2000, message: '{"cloudwatch":"logs2"}'},
      ])

      new_log_stream(today)
      create_log_stream
      put_log_events([
        {timestamp: time_ms + 3000, message: '{"cloudwatch":"logs3"}'},
        {timestamp: time_ms + 4000, message: '{"cloudwatch":"logs4"}'},
      ])

      new_log_stream(yesterday)
      create_log_stream
      put_log_events([
        {timestamp: time_ms + 5000, message: '{"cloudwatch":"logs5"}'},
        {timestamp: time_ms + 6000, message: '{"cloudwatch":"logs6"}'},
      ])

      new_log_stream(tomorrow)
      create_log_stream
      put_log_events([
        {timestamp: time_ms + 7000, message: '{"cloudwatch":"logs7"}'},
        {timestamp: time_ms + 8000, message: '{"cloudwatch":"logs8"}'},
      ])

      new_log_stream(today)
      create_log_stream
      put_log_events([
        {timestamp: time_ms + 9000, message: '{"cloudwatch":"logs9"}'},
        {timestamp: time_ms + 10000, message: '{"cloudwatch":"logs10"}'},
      ])

      new_log_stream(yesterday)
      create_log_stream
      put_log_events([
        {timestamp: time_ms + 11000, message: '{"cloudwatch":"logs11"}'},
        {timestamp: time_ms + 12000, message: '{"cloudwatch":"logs12"}'},
      ])

      sleep 15

      d = create_driver(<<-EOC)
      tag test
      @type cloudwatch_logs
      log_group_name #{log_group_name}
      use_todays_log_stream true
      state_file /tmp/state
      #{aws_key_id}
      #{aws_sec_key}
      #{region}
      #{endpoint}
    EOC
      d.run(expect_emits: 8, timeout: 15)

      emits = d.events
      assert_equal(8, emits.size)
      assert_false(emits.include? ['test', ((time_ms + 1000) / 1000).floor, {'cloudwatch' => 'logs1'}])
      assert_false(emits.include? ['test', ((time_ms + 2000) / 1000).floor, {'cloudwatch' => 'logs2'}])
      assert_true(emits.include? ['test', ((time_ms + 3000) / 1000).floor, {'cloudwatch' => 'logs3'}])
      assert_true(emits.include? ['test', ((time_ms + 4000) / 1000).floor, {'cloudwatch' => 'logs4'}])
      assert_true(emits.include? ['test', ((time_ms + 5000) / 1000).floor, {'cloudwatch' => 'logs5'}])
      assert_true(emits.include? ['test', ((time_ms + 6000) / 1000).floor, {'cloudwatch' => 'logs6'}])
      assert_false(emits.include? ['test', ((time_ms + 7000) / 1000).floor, {'cloudwatch' => 'logs7'}])
      assert_false(emits.include? ['test', ((time_ms + 8000) / 1000).floor, {'cloudwatch' => 'logs8'}])
      assert_true(emits.include? ['test', ((time_ms + 9000) / 1000).floor, {'cloudwatch' => 'logs9'}])
      assert_true(emits.include? ['test', ((time_ms + 10000) / 1000).floor, {'cloudwatch' => 'logs10'}])
      assert_true(emits.include? ['test', ((time_ms + 11000) / 1000).floor, {'cloudwatch' => 'logs11'}])
      assert_true(emits.include? ['test', ((time_ms + 12000) / 1000).floor, {'cloudwatch' => 'logs12'}])
    end
  end

  sub_test_case "stub responses" do
    setup do
      @client = Aws::CloudWatchLogs::Client.new(stub_responses: true)
      mock(Aws::CloudWatchLogs::Client).new(anything) do
        @client
      end
    end

    test "emit" do
      time_ms = (Time.now.to_f * 1000).floor
      log_stream = Aws::CloudWatchLogs::Types::LogStream.new(log_stream_name: "stream_name")
      @client.stub_responses(:describe_log_streams, { log_streams: [log_stream], next_token: nil })
      cloudwatch_logs_events = [
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms, message: { cloudwatch: "logs1" }.to_json, ingestion_time: time_ms),
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms, message: { cloudwatch: "logs2" }.to_json, ingestion_time: time_ms)
      ]
      @client.stub_responses(:get_log_events, { events: cloudwatch_logs_events, next_forward_token: nil })

      d = create_driver
      d.run(expect_emits: 2, timeout: 5)

      events = d.events
      assert_equal(2, events.size)
      assert_equal(["test", (time_ms / 1000), { "cloudwatch" => "logs1" }], events[0])
      assert_equal(["test", (time_ms / 1000), { "cloudwatch" => "logs2" }], events[1])
    end

    test "emit with format" do
      config = <<-CONFIG
        tag test
        @type cloudwatch_logs
        log_group_name #{log_group_name}
        log_stream_name #{log_stream_name}
        state_file /tmp/state
        format /^(?<cloudwatch>[^ ]*)?/
        #{aws_key_id}
        #{aws_sec_key}
        #{region}
        #{endpoint}
      CONFIG
      time_ms = (Time.now.to_f * 1000).floor

      log_stream = Aws::CloudWatchLogs::Types::LogStream.new(log_stream_name: "stream_name")
      @client.stub_responses(:describe_log_streams, { log_streams: [log_stream], next_token: nil })
      cloudwatch_logs_events = [
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms, message: "logs1", ingestion_time: time_ms),
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms, message: "logs2", ingestion_time: time_ms)
      ]
      @client.stub_responses(:get_log_events, { events: cloudwatch_logs_events, next_forward_token: nil })

      d = create_driver(config)
      d.run(expect_emits: 2, timeout: 5)

      events = d.events
      assert_equal(2, events.size)
      assert_equal("test", events[0][0])
      assert_in_delta(time_ms / 1000.0, events[0][1], 1.0)
      assert_equal({ "cloudwatch" => "logs1" }, events[0][2])
      assert_equal("test", events[1][0])
      assert_in_delta(time_ms / 1000.0, events[1][1], 1.0)
      assert_equal({ "cloudwatch" => "logs2" }, events[1][2])
    end

    test "emit with prefix" do
      config = <<-CONFIG
        tag test
        @type cloudwatch_logs
        log_group_name #{log_group_name}
        log_stream_name testprefix
        use_log_stream_name_prefix true
        state_file /tmp/state
        #{aws_key_id}
        #{aws_sec_key}
        #{region}
        #{endpoint}
      CONFIG
      time_ms = (Time.now.to_f * 1000).floor
      log_stream1 = Aws::CloudWatchLogs::Types::LogStream.new(log_stream_name: "stream_name")
      log_stream2 = Aws::CloudWatchLogs::Types::LogStream.new(log_stream_name: "stream_name")
      @client.stub_responses(:describe_log_streams, { log_streams: [log_stream1, log_stream2], next_token: nil })
      cloudwatch_logs_events1 = [
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms + 1000, message: { cloudwatch: "logs1" }.to_json, ingestion_time: time_ms),
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms + 2000, message: { cloudwatch: "logs2" }.to_json, ingestion_time: time_ms)
      ]
      cloudwatch_logs_events2 = [
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms + 3000, message: { cloudwatch: "logs3" }.to_json, ingestion_time: time_ms),
        Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms + 4000, message: { cloudwatch: "logs4" }.to_json, ingestion_time: time_ms)
      ]
      @client.stub_responses(:get_log_events, [
        { events: cloudwatch_logs_events1, next_forward_token: nil },
        { events: cloudwatch_logs_events2, next_forward_token: nil },
      ])

      d = create_driver(config)
      d.run(expect_emits: 4, timeout: 5)

      events = d.events
      assert_equal(4, events.size)
      assert_equal(["test", (time_ms + 1000) / 1000, { "cloudwatch" => "logs1" }], events[0])
      assert_equal(["test", (time_ms + 2000) / 1000, { "cloudwatch" => "logs2" }], events[1])
      assert_equal(["test", (time_ms + 3000) / 1000, { "cloudwatch" => "logs3" }], events[2])
      assert_equal(["test", (time_ms + 4000) / 1000, { "cloudwatch" => "logs4" }], events[3])
    end

    test "emit with today's log stream" do
      config = <<-CONFIG
        tag test
        @type cloudwatch_logs
        log_group_name #{log_group_name}
        use_todays_log_stream true
        state_file /tmp/state
        fetch_interval 0.1
        #{aws_key_id}
        #{aws_sec_key}
        #{region}
        #{endpoint}
      CONFIG

      today = Date.today.strftime("%Y/%m/%d")
      yesterday = (Date.today - 1).strftime("%Y/%m/%d")
      time_ms = (Time.now.to_f * 1000).floor

      log_stream = ->(name) { Aws::CloudWatchLogs::Types::LogStream.new(log_stream_name: "#{name}_#{SecureRandom.uuid}") }
      @client.stub_responses(:describe_log_streams, ->(context) {
        if context.params[:log_stream_name_prefix].start_with?(today)
          { log_streams: [log_stream.call(today)], next_token: nil }
        elsif context.params[:log_stream_name_prefix].start_with?(yesterday)
          { log_streams: [log_stream.call(yesterday)], next_token: nil }
        else
          { log_streams: [], next_token: nil }
        end
      })
      count = 0
      @client.stub_responses(:get_log_events, ->(context) {
        n = count * 2 + 1
        cloudwatch_logs_events = [
          Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms + n * 1000, message: { cloudwatch: "logs#{n}" }.to_json, ingestion_time: time_ms),
          Aws::CloudWatchLogs::Types::OutputLogEvent.new(timestamp: time_ms + (n + 1) * 1000, message: { cloudwatch: "logs#{n + 1}" }.to_json, ingestion_time: time_ms)
        ]
        count += 1
        if context.params[:log_stream_name].start_with?(today)
          { events: cloudwatch_logs_events, next_forward_token: nil }
        elsif context.params[:log_stream_name].start_with?(yesterday)
          { events: cloudwatch_logs_events, next_forward_token: nil }
        else
          flunk("Failed log_stream_name: #{context.params[:log_stream_name]}")
        end
      })

      d = create_driver(config)
      d.run(expect_emits: 8, timeout: 15)

      events = d.events
      assert_equal(8, events.size)
      assert_equal(["test", ((time_ms + 1000) / 1000), { "cloudwatch" => "logs1" }], events[0])
      assert_equal(["test", ((time_ms + 2000) / 1000), { "cloudwatch" => "logs2" }], events[1])
      assert_equal(["test", ((time_ms + 3000) / 1000), { "cloudwatch" => "logs3" }], events[2])
      assert_equal(["test", ((time_ms + 4000) / 1000), { "cloudwatch" => "logs4" }], events[3])
      assert_equal(["test", ((time_ms + 5000) / 1000), { "cloudwatch" => "logs5" }], events[4])
      assert_equal(["test", ((time_ms + 6000) / 1000), { "cloudwatch" => "logs6" }], events[5])
      assert_equal(["test", ((time_ms + 7000) / 1000), { "cloudwatch" => "logs7" }], events[6])
      assert_equal(["test", ((time_ms + 8000) / 1000), { "cloudwatch" => "logs8" }], events[7])
    end
  end

  private

  def default_config
    <<-EOC
      tag test
      @type cloudwatch_logs
      log_group_name #{log_group_name}
      log_stream_name #{log_stream_name}
      state_file /tmp/state
      fetch_interval 1
      #{aws_key_id}
      #{aws_sec_key}
      #{region}
      #{endpoint}
    EOC
  end

  def create_driver(conf = default_config)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::CloudwatchLogsInput).configure(conf)
  end
end
