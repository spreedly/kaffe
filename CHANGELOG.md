# 2.0.0

### Breaking Changes

Compression support by default was removed from Brod. While this allows less dependencies out of the box, it also means that topics with compression now need additional config for `snappy` or `lz4` compression.

To support compression as `brod` did by default before this change, add `snappyer` and `lz4b` to your `deps`, add the following to `config`

```elixir
config :kafka_protocol, provide_compression: [
    snappy: :snappyer,
    lz4: :lz4b
]
```

For more information and to see other supported compression types, see [kafka_protocol's README](https://github.com/kafka4beam/kafka_protocol/blob/master/README.md#compression-support).

### Enhancements

* Bumps `:brod` to v4, which drops `snappyer` dependency requirement

# 1.27.1

### Enhancements

* Updated documentation linking
* Added @impl to common behaviours
* Fixed compiler warnings and formatting
* Created CHANGELOG
