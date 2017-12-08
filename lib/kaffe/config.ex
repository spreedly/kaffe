defmodule Kaffe.Config do

  @doc "Get list of endpoints in brod format"
  @spec endpoints(Keyword.t) :: list({atom, non_neg_integer})
  def endpoints(config) do
    parse_kafka_urls(System.get_env("KAFKA_URL")) || Keyword.get(config, :endpoints, [])
  end

  @doc "Parse Kafka URL(s) into list of endpoints"
  @spec parse_kafka_urls(binary | nil) :: list({atom, non_neg_integer}) | nil
  def parse_kafka_urls(nil), do: nil
  def parse_kafka_urls(urls) do
    for url <- String.split(urls, ","), do: parse_kafka_url(url)
  end

  @doc "Parse Kafka URL into {host, port}"
  @spec parse_kafka_url(binary) :: {atom, non_neg_integer}
  def parse_kafka_url(<<"kafka://", host :: binary>>), do: parse_kafka_host(host, 9092)
  def parse_kafka_url(<<"kafka+ssl://", host ::binary>>), do: parse_kafka_host(host, 9093)

  @spec parse_kafka_host(binary, non_neg_integer) :: {atom, non_neg_integer}
  defp parse_kafka_host(host_port, default_port) do
    case String.split(host_port, ":") do
      [host, port] -> {String.to_atom(host), String.to_integer(port)}
      [host] -> {String.to_atom(host), default_port}
    end
  end

  @doc "Get ssl options from config and OS environment vars"
  @spec ssl(Keyword.t) :: Keyword.t
  def ssl(config) do
    ssl_defaults = config[:ssl] || []
    ssl_options(ssl_cert_option(System.get_env("KAFKA_CLIENT_CERT")) ++
                ssl_key_option(System.get_env("KAFKA_CLIENT_CERT_KEY")) ++
                ssl_cacerts_option(System.get_env("KAFKA_TRUSTED_CERT")) ++
                ssl_defaults)
  end

  @spec ssl_options(Keyword.t) :: Keyword.t
  defp ssl_options([]), do: []
  defp ssl_options(options), do: [ssl: options]

  @spec ssl_cert_option(binary | nil) :: Keyword.t
  def ssl_cert_option(nil), do: []
  def ssl_cert_option(pem) do
    [{_type, der, _cypher_info} | _] = :public_key.pem_decode(pem)
    [cert: der]
  end

  @spec ssl_key_option(binary | nil) :: Keyword.t
  def ssl_key_option(nil), do: []
  def ssl_key_option(pem) do
    [{type, der, _cypher_info} | _] = :public_key.pem_decode(pem)
    [key: {type, der}]
  end

  @spec ssl_cacerts_option(binary | nil) :: Keyword.t
  def ssl_cacerts_option(nil), do: []
  def ssl_cacerts_option(pem) do
    certs = for {_type, der, _cypher_info} <- :public_key.pem_decode(pem), do: der
    [cacerts: certs]
  end

end
