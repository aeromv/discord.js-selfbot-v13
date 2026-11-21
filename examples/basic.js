const { Client } = require('../src/index');

const client = new Client();

client.on('ready', () => {
  console.log(`Logged in as ${client.user.tag} (${client.user.id})`);
  console.log(`Guilds: ${client.guilds.cache.size}`);
  console.log(`Ping: ${client.ws.ping}ms`);
});

client.on('messageCreate', async message => {
  if (message.author.id !== client.user.id) return;

  if (message.content === '!ping') {
    const sent = await message.reply('Pinging...');
    const latency = sent.createdTimestamp - message.createdTimestamp;
    await sent.edit(`Pong! Latency: ${latency}ms | WebSocket: ${client.ws.ping}ms`);
  }

  if (message.content === '!info') {
    message.reply([
      `**User**: ${client.user.tag}`,
      `**Guilds**: ${client.guilds.cache.size}`,
      `**Channels**: ${client.channels.cache.size}`,
      `**Uptime**: ${Math.floor(client.uptime / 1000)}s`,
    ].join('\n'));
  }
});

client.login(process.env.DISCORD_TOKEN);
