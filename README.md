# discord.js-selfbot-v13

Fork of [aiko-chan-ai/discord.js-selfbot-v13](https://github.com/aiko-chan-ai/discord.js-selfbot-v13) with an updated client fingerprint and some fixes to how REST and the gateway behave when things go wrong. The API is unchanged, so code written for upstream should run as-is.

> [!WARNING]
> Selfbots are against Discord's Terms of Service and accounts using them can be terminated. Nothing in this fork changes that.

## Install

The npm name belongs to upstream, so install from GitHub:

```sh
npm install github:aeromv/discord.js-selfbot-v13
```

Requires Node 20.18 or newer. `zlib-sync` is optional; without it the gateway uses Node's built-in zlib.

## Usage

```js
const { Client } = require('discord.js-selfbot-v13');

const client = new Client();

client.on('ready', () => {
  console.log(`logged in as ${client.user.tag}`);
});

client.on('messageCreate', async message => {
  if (message.author.id !== client.user.id) return;

  if (message.content === '!ping') {
    await message.reply(`pong, ${client.ws.ping}ms`);
  }
});

client.login(process.env.DISCORD_TOKEN);
```

Keep the token out of your source. An environment variable or an untracked `.env` file works fine.

There's another example in [`examples/basic.js`](examples/basic.js). For everything else, the upstream docs still apply.

## Differences from upstream

### Client identity

The identify payload and request headers match the stable Windows desktop client: version `1.0.9221`, build `461580`, Windows 10 build `19045`. Gateway capabilities are `30717` instead of `0`. The `sec-ch-ua` header is chosen once per session rather than hardcoded, and the `X-Super-Properties` value is built once and cached.

### REST

- HTTP connections go through a shared undici pool with keep-alive, so most requests don't open a new socket.
- If a GET to the same URL is already in flight, other callers wait on that request instead of sending their own.
- 5xx responses are retried with exponential backoff, starting at 1s and capped at 30s, with some random jitter.
- Sublimit waits get 100 to 500 ms of extra jitter.
- A Cloudflare block (403 with an HTML body and a `cf-ray` header) throws a `CloudFlareError` right away instead of retrying.

### Gateway

- A single missed heartbeat ACK is tolerated. The shard only reconnects after two in a row; upstream reconnected on the first.
- The first heartbeat is sent with a random offset.
- Reconnects back off exponentially from 1s up to 60s.
- The packet queue is capped at 5000. Packets past that are dropped and logged to `debug`.
- Each shard keeps a short latency history, exposed as `shard.metrics.averageLatency`.

### Defaults

| Option | Upstream | This fork |
| --- | --- | --- |
| `restGlobalRateLimit` | `0` | `50` |
| `restTimeOffset` | `500` | `250` |
| `presence.afk` | `true` | `false` |
| `MessageManager` cache | `200` | `100` |
| `PresenceManager` / `ReactionManager` cache | unlimited | `0` |
| `GuildMemberManager` cache | unlimited | `200` |

You can override all of these through `ClientOptions` as usual.

## Contributing

Issues and PRs are welcome. If Discord ships a new client build and the fingerprint falls behind, the values live in `src/util/Options.js` and `src/util/Constants.js`.

## Credits

Built on [aiko-chan-ai/discord.js-selfbot-v13](https://github.com/aiko-chan-ai/discord.js-selfbot-v13), which is built on [discord.js](https://github.com/discordjs/discord.js) v13.

## License

[GPL-3.0](LICENSE), same as upstream.
