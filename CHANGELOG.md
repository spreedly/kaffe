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
