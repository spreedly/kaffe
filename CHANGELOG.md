# 2.2.0

* Adds more brod configuration options: `sleep_timeout`, `prefetch_bytes`, and `prefetch_count`. See `:brod_consumer` for more info
about how these are used.

# 2.1.0

* Removes crc32cer version limit. Bumping to > 1.0.0 requires cmake, so some may want to limit crc32cer themselves in mix.exs.
* Bumps brod and kafka_protocol
* Fixes readme typo
* Fixes e2e test for 2.0.0

# 2.0.0

### Breaking Changes

* Allow keyword configuration for subscribers. Note, keywords require atom keys, so if your current version of `kaffe` is 1.27.0 or higher, adopting to the keyword subscribers is a breaking (and highly encouraged) change.

* Compression support by default was removed from Brod. While this allows less dependencies out of the box, it also means that topics with compression now need additional config for `snappy` or `lz4` compression.

    To support compression as `brod` did by default before this change, add `snappyer` and `lz4b` to your `deps`, add the following to `config`

    ```elixir
    config :kafka_protocol, provide_compression: [
    snappy: :snappyer,
    lz4: :lz4b
    ]
    ```

    For more information and to see other supported compression types, see [kafka_protocol's README](https://github.com/kafka4beam/kafka_protocol/blob/master/README.md#compression).

### Enhancements

* Bumps `:brod` to v4, which drops `snappyer` dependency requirement

# 1.28.0

### Enhancements

* Allow `auto_start_producers` and `allow_topic_auto_creation` to be configurable for brod clients. If configuration of either of these values is desired, update your producer or consumer configs.

* Configures CI to run on pull request.

* Add `Kaffe.MessageHandler` behaviour. To utilize it, add the behaviour to your configured `message_handler` and `@impl Kaffe.MessageHandler` on `handle_messages/1`.

### Fixes

* Stops compiler warnings on duplicate doc definitions
* Fix doc formatting and typos

# 1.27.2

* Relax `:retry` requirement

# 1.27.1

### Enhancements

* Updated documentation linking
* Added @impl to common behaviours
* Fixed compiler warnings and formatting
* Created CHANGELOG
