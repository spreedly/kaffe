defmodule Kaffe.ConfigTest do
  use ExUnit.Case, async: true

  describe "heroku_kafka_endpoints/1" do
    test "transforms endpoints into the correct format" do
      kafka_url = "kafka+ssl://192.168.1.100:9096,kafka+ssl://192.168.1.101:9096,kafka+ssl://192.168.1.102:9096"
      expected = [{:"192.168.1.100", 9096}, {:"192.168.1.101", 9096}, {:"192.168.1.102", 9096}]

      assert Kaffe.Config.heroku_kafka_endpoints(kafka_url) == expected
    end
  end
end
