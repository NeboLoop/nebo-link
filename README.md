# Nebo Link

Link the OpenClaw or Hermes agent you already run to [NeboAI](https://neboai.com), then reach and manage it from the NeboAI phone app and the web. You don't open ports, set up a VPN or configure a tunnel.

Status: early development.

## How it works

`nebo-link` runs next to your agent and makes two outbound connections to NeboAI. One reports that your agent is online. The other is a tunnel that carries your requests from the app to your agent's own local UI. Nothing on your machine listens publicly. Your agent's tokens and passwords stay on your machine, and your NeboAI sign-in sits in front of everything.

## License

MIT
