defmodule Kaffe.ConfigTest do
  use ExUnit.Case, async: false

  describe "endpoints/1" do
    test "parses urls to correct format" do
      assert Kaffe.Config.parse_kafka_url("kafka+ssl://192.168.1.100:9096") == {:"192.168.1.100", 9096}
      assert Kaffe.Config.parse_kafka_url("kafka+ssl://192.168.1.100") == {:"192.168.1.100", 9093}
      assert Kaffe.Config.parse_kafka_url("kafka+ssl://kafka.example.com") == {:"kafka.example.com", 9093}

      assert Kaffe.Config.parse_kafka_url("kafka://192.168.1.100:9096") == {:"192.168.1.100", 9096}
      assert Kaffe.Config.parse_kafka_url("kafka://192.168.1.100") == {:"192.168.1.100", 9092}

      assert Kaffe.Config.parse_kafka_urls(nil) == nil

      kafka_url = "kafka+ssl://192.168.1.100:9096,kafka+ssl://192.168.1.101:9096,kafka+ssl://192.168.1.102:9096"
      expected = [{:"192.168.1.100", 9096}, {:"192.168.1.101", 9096}, {:"192.168.1.102", 9096}]
      assert Kaffe.Config.parse_kafka_urls(kafka_url) == expected
    end

    test "gets endpoint data properly" do
      assert Kaffe.Config.endpoints([endpoints: [kafka: 9096]]) == [kafka: 9096]
    end

    test "gets endpoint data from OS environment" do
      :meck.new(System)
      :meck.expect(System, :get_env, fn("KAFKA_URL") ->
        "kafka+ssl://192.168.1.100:9096,kafka+ssl://192.168.1.101:9096,kafka+ssl://192.168.1.102:9096"
      end)

      assert Kaffe.Config.endpoints([]) == [{:"192.168.1.100", 9096}, {:"192.168.1.101", 9096}, {:"192.168.1.102", 9096}]

      :meck.unload()
    end
  end

  describe "ssl/1" do
    test "ssl returns Erlang SSL module config from OS environment vars" do
      cert_pem = File.read!("./test/data/cert.pem")
      # System.put_env("KAFKA_CLIENT_CERT", cert_pem)

      key_pem = File.read!("./test/data/key.pem")
      # System.put_env("KAFKA_CLIENT_CERT_KEY", key_pem)

      ca_pem = File.read!("./test/data/ca.pem")
      # System.put_env("KAFKA_TRUSTED_CERT", ca_pem)

      cert_der_base64 = File.read!("./test/data/cert.der")
      {:ok, cert_der} = Base.decode64(cert_der_base64, ignore: :whitespace)

      key_der_base64 = File.read!("./test/data/key.der")
      {:ok, key_der} = Base.decode64(key_der_base64, ignore: :whitespace)

      ca_der_base64 = File.read!("./test/data/ca.der")
      {:ok, ca_der} = Base.decode64(ca_der_base64, ignore: :whitespace)

      assert Kaffe.Config.ssl_cert_option(cert_pem) == [cert: cert_der]
      assert Kaffe.Config.ssl_key_option(key_pem) == [key: {:"PrivateKeyInfo", key_der}]
      assert Kaffe.Config.ssl_cacerts_option(ca_pem) == [cacerts: [ca_der]]

      :meck.new(System)
      :meck.expect(System, :get_env,
                   fn
                     ("KAFKA_CLIENT_CERT") -> cert_pem
                     ("KAFKA_CLIENT_CERT_KEY") -> key_pem
                     ("KAFKA_TRUSTED_CERT") -> ca_pem
                   end)

      assert Kaffe.Config.ssl([]) == [
        ssl: [
          cert: cert_der,
          key: {:"PrivateKeyInfo", key_der},
          cacerts: [ca_der],
        ]
      ]

      :meck.unload()
    end
  end
end
