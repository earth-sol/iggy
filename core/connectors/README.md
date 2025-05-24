# Apache Iggy Connectors

Readme is WiP, as of now you can do the following to test the connectors:

1. Build the project in release mode, and make sure that the plugins specified in `core/connectors/config.toml` under `path` are available. BTW you can use either of `toml`, `json` or `yaml` formats.

2. Run `docker compose up -d` from `/core/connectors` which will start the Quickwit server to be used by an example sink connector.

3. Set environment variable `IGGY_CONNECTORS_RUNTIME_CONFIG_PATH=core/connectors/runtime/config` pointing to the runtime configuration file.

4. Start the Iggy server and invoke the following commands to create the example streams and topics used by the connectors.

```
iggy --username iggy --password iggy stream create example
iggy --username iggy --password iggy stream create qw
iggy --username iggy --password iggy topic create qw records 1 none 1d
```

5. Execute `cargo r --bin iggy_connector_data_producer -r` which will start the example data producer application, sending the messages to previously created `qw` stream and `records` topic.

6. Start the connector runtime `cargo r --bin iggy_connector_runtime -r` - you should be able to browse Quickwit UI at `http://localhost:7280` with records being constantly added to the `events` index (this is part of the Quickwit sink). At the same time, you should see the new messages being added to the `example` stream and `topic1` topic by the test source connector - you can use Iggy Web UI to browse the data.

New connector can be built simply by implementing either `Sink` or `Source` trait. Please check the existing examples under `core/connectors/sinks` and `core/connectors/sources` directories.
