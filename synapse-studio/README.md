# synapse-studio

Web dashboard for **[tpt-synapse](https://github.com/tpt-solutions/tpt-synapse)** — browse
topics, queues, and keys, and view broker metrics in a browser.

Built on [axum](https://github.com/tokio-rs/axum). Connects to a running `synapse-broker`
instance over HTTP. Set `SYNAPSE_STUDIO_DEMO=1` to start with an embedded demo broker so
evaluators can explore without running a separate broker process.

## Running

```sh
cargo install synapse-studio
synapse-studio --broker http://localhost:8080
```

## Repository

Full source, architecture docs, and build instructions:
<https://github.com/tpt-solutions/tpt-synapse>
