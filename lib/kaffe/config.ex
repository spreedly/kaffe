defmodule Kaffe.Config do
  def heroku_kafka_endpoints do
    "KAFKA_URL"
    |> System.get_env
    |> heroku_kafka_endpoints
  end

  def heroku_kafka_endpoints(kafka_url) do
    kafka_url
    |> String.replace("kafka+ssl://", "")
    |> String.split(",")
    |> Enum.map(&url_endpoint_to_tuple/1)
  end

  def url_endpoint_to_tuple(endpoint) do
    [ip, port] = endpoint |> String.split(":")
    {ip |> String.to_atom, port |> String.to_integer}
  end

  def ssl_config do
    [
      ssl: [
        cert: client_cert,
        key: client_cert_key,
      ]
    ]
  end

  def client_cert do
    "KAFKA_CLIENT_CERT"
    |> System.get_env
    |> :public_key.pem_decode
    |> List.first
    |> extract_der_cert
  end

  def client_cert_key do
    "KAFKA_CLIENT_CERT_KEY"
    |> System.get_env
    |> :public_key.pem_decode
    |> List.first
    |> extract_type_and_der_cert
  end

  def extract_der_cert({_type, der_cert, _}), do: der_cert

  def extract_type_and_der_cert({type, der_cert, _}), do: {type, der_cert}
end
