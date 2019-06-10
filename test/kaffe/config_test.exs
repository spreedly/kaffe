defmodule Kaffe.ConfigTest do
  use ExUnit.Case, async: true

  describe "heroku_kafka_endpoints/1" do
    test "transforms heroku endpoints into the correct format" do
      System.put_env(
        "KAFKA_URL",
        "kafka+ssl://192.168.1.100:9096,kafka+ssl://192.168.1.101:9096,kafka+ssl://192.168.1.102:9096"
      )

      expected = [{'192.168.1.100', 9096}, {'192.168.1.101', 9096}, {'192.168.1.102', 9096}]

      on_exit(fn ->
        System.delete_env("KAFKA_URL")
      end)

      assert Kaffe.Config.heroku_kafka_endpoints() == expected
    end

    test "transforms endpoints into the correct format" do
      kafka_url = "kafka+ssl://192.168.1.100:9096,kafka+ssl://192.168.1.101:9096,kafka+ssl://192.168.1.102:9096"

      expected = [{'192.168.1.100', 9096}, {'192.168.1.101', 9096}, {'192.168.1.102', 9096}]

      assert Kaffe.Config.parse_endpoints(kafka_url) == expected
    end
  end

  describe "ssl_config/2" do
    test "ssl_config returns an empty list when cert and key are nil" do
      assert Kaffe.Config.ssl_config(nil, nil) == []
    end

    test "ssl_config returns Erlang SSL module config with cert and key" do
      client_cert = "not really a cert"
      client_cert_key = "not really a cert key"

      assert Kaffe.Config.ssl_config(client_cert, client_cert_key) == [
               ssl: [
                 cert: client_cert,
                 key: client_cert_key
               ]
             ]
    end
  end
end
