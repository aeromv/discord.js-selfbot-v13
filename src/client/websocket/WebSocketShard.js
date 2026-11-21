'use strict';

const EventEmitter = require('node:events');
const { createInflate, constants: zlibConstants } = require('node:zlib');
const { setTimeout, setInterval, clearTimeout, clearInterval } = require('node:timers');
const WebSocket = require('../../WebSocket');
const { Status, Events, ShardEvents, Opcodes, WSEvents, WSCodes } = require('../../util/Constants');
const Intents = require('../../util/Intents');
const Util = require('../../util/Util');

const STATUS_KEYS = Object.keys(Status);
const CONNECTION_STATE = Object.keys(WebSocket.WebSocket);

let zlibSync;

try {
  zlibSync = require('zlib-sync');
} catch {} // eslint-disable-line no-empty

const ZLIB_SUFFIX = Buffer.from([0x00, 0x00, 0xff, 0xff]);

/**
 * Represents a Shard's WebSocket connection
 * @extends {EventEmitter}
 */
class WebSocketShard extends EventEmitter {
  constructor(manager, id) {
    super();

    /**
     * The WebSocketManager of the shard
     * @type {WebSocketManager}
     */
    this.manager = manager;

    /**
     * The shard's id
     * @type {number}
     */
    this.id = id;

    /**
     * The resume URL for this shard
     * @type {?string}
     * @private
     */
    this.resumeURL = null;

    /**
     * The current status of the shard
     * @type {Status}
     */
    this.status = Status.IDLE;

    /**
     * The current sequence of the shard
     * @type {number}
     * @private
     */
    this.sequence = -1;

    /**
     * The sequence of the shard after close
     * @type {number}
     * @private
     */
    this.closeSequence = 0;

    /**
     * The current session id of the shard
     * @type {?string}
     * @private
     */
    this.sessionId = null;

    /**
     * The previous heartbeat ping of the shard
     * @type {number}
     */
    this.ping = -1;

    /**
     * The last time a ping was sent (a timestamp)
     * @type {number}
     * @private
     */
    this.lastPingTimestamp = -1;

    /**
     * If we received a heartbeat ack back. Used to identify zombie connections
     * @type {boolean}
     * @private
     */
    this.lastHeartbeatAcked = true;

    /**
     * Used to prevent calling {@link WebSocketShard#event:close} twice while closing or terminating the WebSocket.
     * @type {boolean}
     * @private
     */
    this.closeEmitted = false;

    /**
     * Number of consecutive missed heartbeat ACKs before zombie detection
     * @type {number}
     * @private
     */
    this.missedHeartbeats = 0;

    /**
     * Connection quality metrics
     * @type {Object}
     */
    this.metrics = {
      latencyHistory: [],
      averageLatency: -1,
      minLatency: Infinity,
      maxLatency: 0,
      totalPacketsReceived: 0,
      totalHeartbeatsSent: 0,
      totalHeartbeatsMissed: 0,
      connectionUptime: 0,
      lastConnectionAt: 0,
      reconnectCount: 0,
    };

    /**
     * Current reconnect attempt number for exponential backoff
     * @type {number}
     * @private
     */
    this.reconnectAttempts = 0;

    /**
     * Contains the rate limit queue and metadata
     * @name WebSocketShard#ratelimit
     * @type {Object}
     * @private
     */
    Object.defineProperty(this, 'ratelimit', {
      value: {
        queue: [],
        total: 120,
        remaining: 120,
        time: 60e3,
        timer: null,
      },
    });

    /**
     * The WebSocket connection for the current shard
     * @name WebSocketShard#connection
     * @type {?WebSocket}
     * @private
     */
    Object.defineProperty(this, 'connection', { value: null, writable: true });

    /**
     * @external Inflate
     * @see {@link https://www.npmjs.com/package/zlib-sync}
     */

    /**
     * The compression to use
     * @name WebSocketShard#inflate
     * @type {?Inflate}
     * @private
     */
    Object.defineProperty(this, 'inflate', { value: null, writable: true });

    /**
     * Buffer chunks for native zlib decompression
     * @name WebSocketShard#zlibChunks
     * @type {?Buffer[]}
     * @private
     */
    Object.defineProperty(this, 'zlibChunks', { value: null, writable: true });

    /**
     * The HELLO timeout
     * @name WebSocketShard#helloTimeout
     * @type {?NodeJS.Timeout}
     * @private
     */
    Object.defineProperty(this, 'helloTimeout', { value: null, writable: true });

    /**
     * The WebSocket timeout.
     * @name WebSocketShard#wsCloseTimeout
     * @type {?NodeJS.Timeout}
     * @private
     */
    Object.defineProperty(this, 'wsCloseTimeout', { value: null, writable: true });

    /**
     * If the manager attached its event handlers on the shard
     * @name WebSocketShard#eventsAttached
     * @type {boolean}
     * @private
     */
    Object.defineProperty(this, 'eventsAttached', { value: false, writable: true });

    /**
     * A set of guild ids this shard expects to receive
     * @name WebSocketShard#expectedGuilds
     * @type {?Set<string>}
     * @private
     */
    Object.defineProperty(this, 'expectedGuilds', { value: null, writable: true });

    /**
     * The ready timeout
     * @name WebSocketShard#readyTimeout
     * @type {?NodeJS.Timeout}
     * @private
     */
    Object.defineProperty(this, 'readyTimeout', { value: null, writable: true });

    /**
     * Time when the WebSocket connection was opened
     * @name WebSocketShard#connectedAt
     * @type {number}
     * @private
     */
    Object.defineProperty(this, 'connectedAt', { value: 0, writable: true });
  }

  /**
   * Emits a debug event.
   * @param {string} message The debug message
   * @private
   */
  debug(message) {
    this.manager.debug(message, this);
  }

  /**
   * Connects the shard to the gateway.
   * @private
   * @returns {Promise<void>} A promise that will resolve if the shard turns ready successfully,
   * or reject if we couldn't connect
   */
  connect() {
    const { client } = this.manager;

    if (this.connection?.readyState === WebSocket.OPEN && this.status === Status.READY) {
      return Promise.resolve();
    }

    const gateway = this.resumeURL ?? this.manager.gateway;

    return new Promise((resolve, reject) => {
      const cleanup = () => {
        this.removeListener(ShardEvents.CLOSE, onClose);
        this.removeListener(ShardEvents.READY, onReady);
        this.removeListener(ShardEvents.RESUMED, onResumed);
        this.removeListener(ShardEvents.INVALID_SESSION, onInvalidOrDestroyed);
        this.removeListener(ShardEvents.DESTROYED, onInvalidOrDestroyed);
      };

      const onReady = () => {
        cleanup();
        this.reconnectAttempts = 0;
        resolve();
      };

      const onResumed = () => {
        cleanup();
        this.reconnectAttempts = 0;
        resolve();
      };

      const onClose = event => {
        cleanup();
        reject(event);
      };

      const onInvalidOrDestroyed = () => {
        cleanup();
        // eslint-disable-next-line prefer-promise-reject-errors
        reject();
      };

      this.once(ShardEvents.READY, onReady);
      this.once(ShardEvents.RESUMED, onResumed);
      this.once(ShardEvents.CLOSE, onClose);
      this.once(ShardEvents.INVALID_SESSION, onInvalidOrDestroyed);
      this.once(ShardEvents.DESTROYED, onInvalidOrDestroyed);

      if (this.connection?.readyState === WebSocket.OPEN) {
        this.debug('An open connection was found, attempting an immediate identify.');
        this.identify();
        return;
      }

      if (this.connection) {
        this.debug(`A connection object was found. Cleaning up before continuing.
    State: ${CONNECTION_STATE[this.connection.readyState]}`);
        this.destroy({ emit: false });
      }

      const wsQuery = { v: client.options.ws.version };

      this._setupCompression(wsQuery);

      this.debug(
        `[CONNECT]
    Gateway    : ${gateway}
    Version    : ${client.options.ws.version}
    Encoding   : ${WebSocket.encoding}
    Compression: ${zlibSync ? 'zlib-sync' : this.zlibChunks !== null ? 'zlib-native' : 'none'}
    Agent      : ${Util.verifyProxyAgent(client.options.ws.agent)}`,
      );

      this.status = this.status === Status.DISCONNECTED ? Status.RECONNECTING : Status.CONNECTING;
      this.setHelloTimeout();
      this.setWsCloseTimeout(-1);
      this.connectedAt = Date.now();
      this.metrics.lastConnectionAt = this.connectedAt;

      const ws = (this.connection = WebSocket.create(gateway, wsQuery, {
        handshakeTimeout: 30_000,
        agent: Util.verifyProxyAgent(client.options.ws.agent) ? client.options.ws.agent : undefined,
      }));
      ws.onopen = this.onOpen.bind(this);
      ws.onmessage = this.onMessage.bind(this);
      ws.onerror = this.onError.bind(this);
      ws.onclose = this.onClose.bind(this);
    });
  }

  /**
   * Sets up zlib compression, preferring zlib-sync and falling back to native zlib.
   * @param {Object} wsQuery The WebSocket query parameters to mutate
   * @private
   */
  _setupCompression(wsQuery) {
    if (zlibSync) {
      this.inflate = new zlibSync.Inflate({
        chunkSize: 128 * 1024,
        flush: zlibSync.Z_SYNC_FLUSH,
        to: WebSocket.encoding === 'json' ? 'string' : '',
      });
      this.zlibChunks = null;
      wsQuery.compress = 'zlib-stream';
    } else if (this.manager.client.options.ws.compress !== false) {
      this.inflate = null;
      this.zlibChunks = [];
      wsQuery.compress = 'zlib-stream';
    } else {
      this.inflate = null;
      this.zlibChunks = null;
    }
  }

  /**
   * Decompresses data received from the gateway using native node:zlib.
   * @param {Buffer} data The compressed data chunks
   * @returns {?string|Buffer} The decompressed data, or null if not ready
   * @private
   */
  _nativeZlibDecompress(data) {
    this.zlibChunks.push(Buffer.isBuffer(data) ? data : Buffer.from(data));

    const l = data.length;
    const isFlushed =
      l >= 4 && data[l - 4] === 0x00 && data[l - 3] === 0x00 && data[l - 2] === 0xff && data[l - 1] === 0xff;

    if (!isFlushed) return null;

    const combined = Buffer.concat(this.zlibChunks);
    this.zlibChunks.length = 0;

    try {
      const inflate = createInflate();
      const chunks = [];
      return new Promise((resolve, reject) => {
        inflate.on('data', chunk => chunks.push(chunk));
        inflate.on('end', () => {
          const result = Buffer.concat(chunks);
          resolve(WebSocket.encoding === 'json' ? result.toString() : result);
        });
        inflate.on('error', reject);
        inflate.end(combined);
      });
    } catch (err) {
      this.debug(`[ZLIB] Native decompression error: ${err.message}`);
      this.zlibChunks.length = 0;
      return null;
    }
  }

  /**
   * Called whenever a connection is opened to the gateway.
   * @private
   */
  onOpen() {
    const connectionTime = Date.now() - this.connectedAt;
    this.debug(`[CONNECTED] Took ${connectionTime}ms`);
    this.status = Status.NEARLY;
  }

  /**
   * Called whenever a message is received.
   * @param {MessageEvent} event Event received
   * @private
   */
  onMessage({ data }) {
    this.metrics.totalPacketsReceived++;

    let raw;
    if (data instanceof ArrayBuffer) data = new Uint8Array(data);

    if (zlibSync && this.inflate) {
      const l = data.length;
      const flush =
        l >= 4 && data[l - 4] === 0x00 && data[l - 3] === 0x00 && data[l - 2] === 0xff && data[l - 1] === 0xff;

      this.inflate.push(data, flush && zlibSync.Z_SYNC_FLUSH);
      if (!flush) return;
      raw = this.inflate.result;
    } else if (this.zlibChunks !== null) {
      const result = this._nativeZlibDecompress(data);
      if (result === null) return;
      if (result instanceof Promise) {
        result
          .then(decompressed => this._processRawMessage(decompressed))
          .catch(err => this.manager.client.emit(Events.SHARD_ERROR, err, this.id));
        return;
      }
      raw = result;
    } else {
      raw = data;
    }

    this._processRawMessage(raw);
  }

  /**
   * Processes a raw (decompressed) message from the gateway.
   * @param {string|Buffer} raw The raw message data
   * @private
   */
  _processRawMessage(raw) {
    let packet;
    try {
      packet = WebSocket.unpack(raw);
    } catch (err) {
      this.manager.client.emit(Events.SHARD_ERROR, err, this.id);
      return;
    }
    this.manager.client.emit(Events.RAW, packet, this.id);
    if (packet.op === Opcodes.DISPATCH) this.manager.emit(packet.t, packet.d, this.id);
    this.onPacket(packet);
  }

  /**
   * Called whenever an error occurs with the WebSocket.
   * @param {ErrorEvent} event The error that occurred
   * @private
   */
  onError(event) {
    const error = event?.error ?? event;
    if (!error) return;

    /**
     * Emitted whenever a shard's WebSocket encounters a connection error.
     * @event Client#shardError
     * @param {Error} error The encountered error
     * @param {number} shardId The shard that encountered this error
     */
    this.manager.client.emit(Events.SHARD_ERROR, error, this.id);
  }

  /**
   * @external CloseEvent
   * @see {@link https://developer.mozilla.org/docs/Web/API/CloseEvent}
   */

  /**
   * @external ErrorEvent
   * @see {@link https://developer.mozilla.org/docs/Web/API/ErrorEvent}
   */

  /**
   * @external MessageEvent
   * @see {@link https://developer.mozilla.org/docs/Web/API/MessageEvent}
   */

  /**
   * Called whenever a connection to the gateway is closed.
   * @param {CloseEvent} event Close event that was received
   * @private
   */
  onClose(event) {
    this.closeEmitted = true;
    if (this.sequence !== -1) this.closeSequence = this.sequence;
    this.sequence = -1;
    this.setHeartbeatTimer(-1);
    this.setHelloTimeout(-1);
    this.setWsCloseTimeout(-1);
    if (this.connection) {
      this._cleanupConnection();
      this.destroy({ reset: !this.sessionId, emit: false, log: false });
    }
    this.status = Status.DISCONNECTED;

    this.metrics.connectionUptime += Date.now() - this.metrics.lastConnectionAt;

    this.emitClose(event);
  }

  /**
   * This method is responsible to emit close event for this shard.
   * This method helps the shard reconnect.
   * @param {CloseEvent} [event] Close event that was received
   */
  emitClose(
    event = {
      code: 1011,
      reason: WSCodes[1011],
      wasClean: false,
    },
  ) {
    this.debug(`[CLOSE]
    Event Code: ${event.code}
    Clean     : ${event.wasClean}
    Reason    : ${event.reason ?? 'No reason received'}`);
    /**
     * Emitted when a shard's WebSocket closes.
     * @private
     * @event WebSocketShard#close
     * @param {CloseEvent} event The received event
     */
    this.emit(ShardEvents.CLOSE, event);
  }

  /**
   * Called whenever a packet is received.
   * @param {Object} packet The received packet
   * @private
   */
  onPacket(packet) {
    if (!packet) {
      this.debug(`Received broken packet: '${packet}'.`);
      return;
    }

    switch (packet.t) {
      case WSEvents.READY:
        /**
         * Emitted when the shard receives the READY payload and is now waiting for guilds
         * @event WebSocketShard#ready
         */
        this.emit(ShardEvents.READY);

        this.resumeURL = packet.d.resume_gateway_url;
        this.sessionId = packet.d.session_id;
        this.expectedGuilds = new Set(packet.d.guilds.filter(d => d?.unavailable == true).map(d => d.id));
        this.status = Status.WAITING_FOR_GUILDS;
        this.debug(`[READY] Session ${this.sessionId} | Resume url ${this.resumeURL}.`);
        this.lastHeartbeatAcked = true;
        this.missedHeartbeats = 0;
        this.sendHeartbeat('ReadyHeartbeat');
        break;
      case WSEvents.RESUMED: {
        /**
         * Emitted when the shard resumes successfully
         * @event WebSocketShard#resumed
         */
        this.emit(ShardEvents.RESUMED);

        this.status = Status.READY;
        const replayed = packet.s - this.closeSequence;
        this.debug(`[RESUMED] Session ${this.sessionId} | Replayed ${replayed} events.`);
        this.lastHeartbeatAcked = true;
        this.missedHeartbeats = 0;
        this.sendHeartbeat('ResumeHeartbeat');
        break;
      }
    }

    if (packet.s > this.sequence) this.sequence = packet.s;

    switch (packet.op) {
      case Opcodes.HELLO:
        this.setHelloTimeout(-1);
        this.setHeartbeatTimer(packet.d.heartbeat_interval);
        this.identify();
        break;
      case Opcodes.RECONNECT:
        this.debug('[RECONNECT] Discord asked us to reconnect');
        this.destroy({ closeCode: 4_000 });
        break;
      case Opcodes.INVALID_SESSION:
        this.debug(`[INVALID SESSION] Resumable: ${packet.d}.`);
        if (packet.d) {
          this.identifyResume();
          return;
        }
        this.sequence = -1;
        this.sessionId = null;
        this.status = Status.RECONNECTING;
        /**
         * Emitted when the session has been invalidated.
         * @event WebSocketShard#invalidSession
         */
        this.emit(ShardEvents.INVALID_SESSION);
        break;
      case Opcodes.HEARTBEAT_ACK:
        this.ackHeartbeat();
        break;
      case Opcodes.HEARTBEAT:
        this.sendHeartbeat('HeartbeatRequest', true);
        break;
      default:
        this.manager.handlePacket(packet, this);
        if (this.status === Status.WAITING_FOR_GUILDS && packet.t === WSEvents.GUILD_CREATE) {
          this.expectedGuilds.delete(packet.d.id);
          this.checkReady();
        }
    }
  }

  /**
   * Checks if the shard can be marked as ready
   * @private
   */
  checkReady() {
    if (this.readyTimeout) {
      clearTimeout(this.readyTimeout);
      this.readyTimeout = null;
    }
    if (!this.expectedGuilds.size) {
      this.debug('Shard received all its guilds. Marking as fully ready.');
      this.status = Status.READY;

      /**
       * Emitted when the shard is fully ready.
       * This event is emitted if:
       * * all guilds were received by this shard
       * * the ready timeout expired, and some guilds are unavailable
       * @event WebSocketShard#allReady
       * @param {?Set<string>} unavailableGuilds Set of unavailable guilds, if any
       */
      this.emit(ShardEvents.ALL_READY);
      return;
    }
    const hasGuildsIntent = new Intents(this.manager.client.options.intents).has(Intents.FLAGS.GUILDS);

    const { waitGuildTimeout } = this.manager.client.options;

    this.readyTimeout = setTimeout(
      () => {
        this.debug(
          `Shard ${hasGuildsIntent ? 'did' : 'will'} not receive any more guild packets` +
            `${hasGuildsIntent ? ` in ${waitGuildTimeout} ms` : ''}.\nUnavailable guild count: ${
              this.expectedGuilds.size
            }`,
        );

        this.readyTimeout = null;

        this.status = Status.READY;

        this.emit(ShardEvents.ALL_READY, this.expectedGuilds);
      },
      hasGuildsIntent ? waitGuildTimeout : 0,
    ).unref();
  }

  /**
   * Sets the HELLO packet timeout.
   * @param {number} [time] If set to -1, it will clear the hello timeout
   * @private
   */
  setHelloTimeout(time) {
    if (time === -1) {
      if (this.helloTimeout) {
        this.debug('Clearing the HELLO timeout.');
        clearTimeout(this.helloTimeout);
        this.helloTimeout = null;
      }
      return;
    }
    this.debug('Setting a HELLO timeout for 20s.');
    this.helloTimeout = setTimeout(() => {
      this.debug('Did not receive HELLO in time. Destroying and connecting again.');
      this.destroy({ reset: true, closeCode: 4009 });
    }, 20_000).unref();
  }

  /**
   * Sets the WebSocket Close timeout.
   * This method is responsible for detecting any zombie connections if the WebSocket fails to close properly.
   * @param {number} [time] If set to -1, it will clear the timeout
   * @private
   */
  setWsCloseTimeout(time) {
    if (this.wsCloseTimeout) {
      this.debug('[WebSocket] Clearing the close timeout.');
      clearTimeout(this.wsCloseTimeout);
    }
    if (time === -1) {
      this.wsCloseTimeout = null;
      return;
    }
    this.wsCloseTimeout = setTimeout(() => {
      this.setWsCloseTimeout(-1);

      if (this.closeEmitted) {
        this.debug(`[WebSocket] close was already emitted, assuming the connection was closed properly.`);
        this.closeEmitted = false;
        return;
      }

      this.debug(
        `[WebSocket] Close Emitted: ${this.closeEmitted} | did not close properly, assuming a zombie connection.\nEmitting close and reconnecting again.`,
      );

      if (this.connection) this._cleanupConnection();

      this.emitClose({
        code: 4009,
        reason: 'Session time out.',
        wasClean: false,
      });
    }, time);
  }

  /**
   * Sets the heartbeat timer for this shard.
   * @param {number} time If -1, clears the interval, any other number sets an interval
   * @private
   */
  setHeartbeatTimer(time) {
    if (time === -1) {
      if (this.heartbeatInterval) {
        this.debug('Clearing the heartbeat interval.');
        clearInterval(this.heartbeatInterval);
        this.heartbeatInterval = null;
      }
      return;
    }
    this.debug(`Setting a heartbeat interval for ${time}ms.`);
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
    // Add jitter to heartbeat interval like the real Discord client does
    const jitter = Math.random();
    const firstHeartbeat = Math.floor(time * jitter);
    this.debug(`First heartbeat in ${firstHeartbeat}ms (jitter: ${jitter.toFixed(3)})`);

    // Send the first heartbeat after jitter, then switch to regular interval
    this.heartbeatInterval = setTimeout(() => {
      this.sendHeartbeat();
      this.heartbeatInterval = setInterval(() => this.sendHeartbeat(), time).unref();
    }, firstHeartbeat).unref();
  }

  /**
   * Sends a heartbeat to the WebSocket.
   * If this shard didn't receive a heartbeat last time, it will destroy it and reconnect
   * @param {string} [tag='HeartbeatTimer'] What caused this heartbeat to be sent
   * @param {boolean} [ignoreHeartbeatAck] If we should send the heartbeat forcefully.
   * @private
   */
  sendHeartbeat(
    tag = 'HeartbeatTimer',
    ignoreHeartbeatAck = [Status.WAITING_FOR_GUILDS, Status.IDENTIFYING, Status.RESUMING].includes(this.status),
  ) {
    this.metrics.totalHeartbeatsSent++;

    if (ignoreHeartbeatAck && !this.lastHeartbeatAcked) {
      this.debug(`[${tag}] Didn't process heartbeat ack yet but we are still connected. Sending one now.`);
    } else if (!this.lastHeartbeatAcked) {
      this.missedHeartbeats++;
      this.metrics.totalHeartbeatsMissed++;

      // Allow one missed heartbeat before declaring zombie — network hiccups happen
      if (this.missedHeartbeats >= 2) {
        this.debug(
          `[${tag}] Missed ${this.missedHeartbeats} heartbeat ACKs, assuming zombie connection. Destroying and reconnecting.
    Status          : ${STATUS_KEYS[this.status]}
    Sequence        : ${this.sequence}
    Connection State: ${this.connection ? CONNECTION_STATE[this.connection.readyState] : 'No Connection??'}
    Avg Latency     : ${this.metrics.averageLatency.toFixed(0)}ms`,
        );
        this.missedHeartbeats = 0;
        this.destroy({ reset: true, closeCode: 4009 });
        return;
      }

      this.debug(`[${tag}] Missed ${this.missedHeartbeats} heartbeat ACK(s), will retry once more before zombie detection.`);
    }

    this.debug(`[${tag}] Sending a heartbeat.`);
    this.lastHeartbeatAcked = false;
    this.lastPingTimestamp = Date.now();
    this.send({ op: Opcodes.HEARTBEAT, d: this.sequence }, true);
  }

  /**
   * Acknowledges a heartbeat.
   * @private
   */
  ackHeartbeat() {
    this.lastHeartbeatAcked = true;
    this.missedHeartbeats = 0;
    const latency = Date.now() - this.lastPingTimestamp;
    this.debug(`Heartbeat acknowledged, latency of ${latency}ms.`);
    this.ping = latency;

    // Track latency history (keep last 20 samples)
    this.metrics.latencyHistory.push(latency);
    if (this.metrics.latencyHistory.length > 20) {
      this.metrics.latencyHistory.shift();
    }
    this.metrics.averageLatency =
      this.metrics.latencyHistory.reduce((a, b) => a + b, 0) / this.metrics.latencyHistory.length;
    this.metrics.minLatency = Math.min(this.metrics.minLatency, latency);
    this.metrics.maxLatency = Math.max(this.metrics.maxLatency, latency);
  }

  /**
   * Identifies the client on the connection.
   * @private
   * @returns {void}
   */
  identify() {
    return this.sessionId ? this.identifyResume() : this.identifyNew();
  }

  /**
   * Identifies as a new connection on the gateway.
   * @private
   */
  identifyNew() {
    const { client } = this.manager;
    if (!client.token) {
      this.debug('[IDENTIFY] No token available to identify a new session.');
      return;
    }

    this.status = Status.IDENTIFYING;

    // Patch legacy $-prefixed property keys
    Object.keys(client.options.ws.properties)
      .filter(k => k.startsWith('$'))
      .forEach(k => {
        client.options.ws.properties[k.slice(1)] = client.options.ws.properties[k];
        delete client.options.ws.properties[k];
      });

    const d = {
      ...client.options.ws,
      token: client.token,
    };

    delete d.version;
    delete d.agent;

    this.debug(`[IDENTIFY] Shard ${this.id} | Capabilities: ${d.capabilities}`);
    this.send({ op: Opcodes.IDENTIFY, d }, true);
  }

  /**
   * Resumes a session on the gateway.
   * @private
   */
  identifyResume() {
    if (!this.sessionId) {
      this.debug('[RESUME] No session id was present; identifying as a new session.');
      this.identifyNew();
      return;
    }

    this.status = Status.RESUMING;
    this.metrics.reconnectCount++;

    this.debug(`[RESUME] Session ${this.sessionId}, sequence ${this.closeSequence}`);

    const d = {
      token: this.manager.client.token,
      session_id: this.sessionId,
      seq: this.closeSequence,
    };

    this.send({ op: Opcodes.RESUME, d }, true);
  }

  /**
   * Calculates the reconnect delay using exponential backoff with jitter.
   * @returns {number} Delay in milliseconds
   * @private
   */
  getReconnectDelay() {
    const base = 1000;
    const maxDelay = 60_000;
    const exponential = Math.min(base * Math.pow(2, this.reconnectAttempts), maxDelay);
    // Full jitter: random between 0 and exponential ceiling
    const jitter = Math.random() * exponential;
    this.reconnectAttempts++;
    return Math.floor(jitter);
  }

  /**
   * Adds a packet to the queue to be sent to the gateway.
   * <warn>If you use this method, make sure you understand that you need to provide
   * a full [Payload](https://discord.com/developers/docs/topics/gateway-events#payload-structure).
   * Do not use this method if you don't know what you're doing.</warn>
   * @param {Object} data The full packet to send
   * @param {boolean} [important=false] If this packet should be added first in queue
   */
  send(data, important = false) {
    this.ratelimit.queue[important ? 'unshift' : 'push'](data);
    this.processQueue();
  }

  /**
   * Sends data, bypassing the queue.
   * @param {Object} data Packet to send
   * @returns {void}
   * @private
   */
  _send(data) {
    if (this.connection?.readyState !== WebSocket.OPEN) {
      this.debug(`Tried to send packet '${JSON.stringify(data)}' but no WebSocket is available!`);
      this.destroy({ closeCode: 4_000 });
      return;
    }

    this.connection.send(WebSocket.pack(data), err => {
      if (err) this.manager.client.emit(Events.SHARD_ERROR, err, this.id);
    });
  }

  /**
   * Processes the current WebSocket queue.
   * @returns {void}
   * @private
   */
  processQueue() {
    if (this.ratelimit.remaining === 0) return;
    if (this.ratelimit.queue.length === 0) return;
    if (this.ratelimit.remaining === this.ratelimit.total) {
      this.ratelimit.timer = setTimeout(() => {
        this.ratelimit.remaining = this.ratelimit.total;
        this.processQueue();
      }, this.ratelimit.time).unref();
    }
    while (this.ratelimit.remaining > 0) {
      const item = this.ratelimit.queue.shift();
      if (!item) return;
      this._send(item);
      this.ratelimit.remaining--;
    }
  }

  /**
   * Destroys this shard and closes its WebSocket connection.
   * @param {Object} [options={ closeCode: 1000, reset: false, emit: true, log: true }] Options for destroying the shard
   * @private
   */
  destroy({ closeCode = 1_000, reset = false, emit = true, log = true } = {}) {
    if (log) {
      this.debug(`[DESTROY]
    Close Code    : ${closeCode}
    Reset         : ${reset}
    Emit DESTROYED: ${emit}`);
    }

    this.setHeartbeatTimer(-1);
    this.setHelloTimeout(-1);

    this.debug(
      `[WebSocket] Destroy: Attempting to close the WebSocket. | WS State: ${
        CONNECTION_STATE[this.connection?.readyState ?? WebSocket.CLOSED]
      }`,
    );

    if (this.connection) {
      if (this.connection.readyState === WebSocket.OPEN) {
        this.connection.close(closeCode);
        this.debug(`[WebSocket] Close: Tried closing. | WS State: ${CONNECTION_STATE[this.connection.readyState]}`);
      } else {
        this.debug(`WS State: ${CONNECTION_STATE[this.connection.readyState]}`);
        try {
          this.connection.close(closeCode);
        } catch (err) {
          this.debug(
            `[WebSocket] Close: Something went wrong while closing the WebSocket: ${
              err.message || err
            }. Forcefully terminating the connection | WS State: ${CONNECTION_STATE[this.connection.readyState]}`,
          );
          this.connection.terminate();
        }
        if (emit) this._emitDestroyed();
      }
    } else if (emit) {
      this._emitDestroyed();
    }

    this.debug(
      `[WebSocket] Adding a WebSocket close timeout to ensure a correct WS reconnect.
        Timeout: ${this.manager.client.options.closeTimeout}ms`,
    );
    this.setWsCloseTimeout(this.manager.client.options.closeTimeout);

    this.connection = null;

    this.status = Status.DISCONNECTED;

    if (this.sequence !== -1) this.closeSequence = this.sequence;

    if (reset) {
      this.resumeURL = null;
      this.sequence = -1;
      this.sessionId = null;
    }

    this.ratelimit.remaining = this.ratelimit.total;
    this.ratelimit.queue.length = 0;
    if (this.ratelimit.timer) {
      clearTimeout(this.ratelimit.timer);
      this.ratelimit.timer = null;
    }
  }

  /**
   * Cleans up the WebSocket connection listeners.
   * @private
   */
  _cleanupConnection() {
    this.connection.onopen = this.connection.onclose = this.connection.onmessage = null;
    this.connection.onerror = () => null;
  }

  /**
   * Emits the DESTROYED event on the shard
   * @private
   */
  _emitDestroyed() {
    /**
     * Emitted when a shard is destroyed, but no WebSocket connection was present.
     * @private
     * @event WebSocketShard#destroyed
     */
    this.emit(ShardEvents.DESTROYED);
  }
}

module.exports = WebSocketShard;
