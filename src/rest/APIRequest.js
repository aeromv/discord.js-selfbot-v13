'use strict';

const Buffer = require('node:buffer').Buffer;
const { setTimeout } = require('node:timers');
const { FormData, buildConnector, Client, ProxyAgent } = require('undici');
const { ciphers } = require('../util/Constants');
const Util = require('../util/Util');

let agent = null;

// Per-session sec-ch-ua randomization — pick a Not-A-Brand version once and reuse it
const NOT_A_BRAND_VERSIONS = ['8', '24', '99'];
const sessionBrandVersion = NOT_A_BRAND_VERSIONS[Math.floor(Math.random() * NOT_A_BRAND_VERSIONS.length)];

// In-flight GET request deduplication
const inflightGets = new Map();

class APIRequest {
  constructor(rest, method, path, options) {
    this.rest = rest;
    this.client = rest.client;
    this.method = method;
    this.route = options.route;
    this.options = options;
    this.retries = 0;

    this.fullUserAgent = this.client.options.http.headers['User-Agent'];
    this.client.options.ws.properties.browser_user_agent = this.fullUserAgent;

    // Cache the super-properties encoding per session, only rebuild when properties change
    if (!this.client._superPropertiesCache) {
      this._rebuildSuperProperties();
    }

    let queryString = '';
    if (options.query) {
      const query = Object.entries(options.query)
        .filter(([, value]) => value !== null && typeof value !== 'undefined')
        .flatMap(([key, value]) => (Array.isArray(value) ? value.map(v => [key, v]) : [[key, value]]));
      queryString = new URLSearchParams(query).toString();
    }
    this.path = `${path}${queryString && `?${queryString}`}`;
  }

  _rebuildSuperProperties() {
    const props = this.client.options.ws.properties;
    this.client._superPropertiesCache = Buffer.from(JSON.stringify(props), 'ascii').toString('base64');
  }

  static _getAgent(clientOptions) {
    if (!agent) {
      const r_ = Util.checkUndiciProxyAgent(clientOptions.http.agent);
      if (!r_) {
        agent = new Client('https://discord.com', {
          connect: buildConnector({ ciphers: ciphers.join(':') }),
          keepAliveTimeout: 30_000,
          keepAliveMaxTimeout: 60_000,
          pipelining: 1,
        });
      } else {
        agent = new ProxyAgent({
          ...r_,
          ciphers: ciphers.join(':'),
        });
      }
    }
    return agent;
  }

  // Reset the shared agent (useful when proxy settings change)
  static resetAgent() {
    if (agent) {
      agent.close?.();
      agent = null;
    }
  }

  make(captchaKey, captchaRqToken) {
    const dispatcher = APIRequest._getAgent(this.client.options);

    const API =
      this.options.versioned === false
        ? this.client.options.http.api
        : `${this.client.options.http.api}/v${this.client.options.http.version}`;
    const url = API + this.path;

    let headers = {
      accept: '*/*',
      'accept-language': 'en-US,en;q=0.9',
      priority: 'u=1, i',
      referer: 'https://discord.com/channels/@me',
      'sec-ch-ua': `"Not A(Brand";v="${sessionBrandVersion}", "Chromium";v="134"`,
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"Windows"',
      'sec-fetch-dest': 'empty',
      'sec-fetch-mode': 'cors',
      'sec-fetch-site': 'same-origin',
      'x-discord-locale': 'en-US',
      'x-discord-timezone': Intl.DateTimeFormat().resolvedOptions().timeZone,
      'x-super-properties': this.client._superPropertiesCache,
      origin: 'https://discord.com',
      'x-debug-options': 'bugReporterEnabled',
      ...this.client.options.http.headers,
      'User-Agent': this.fullUserAgent,
    };

    if (this.options.auth !== false) headers.Authorization = this.rest.getAuth();
    if (this.options.reason) headers['X-Audit-Log-Reason'] = encodeURIComponent(this.options.reason);
    if (this.options.headers) headers = Object.assign(headers, this.options.headers);

    for (const [key, value] of Object.entries(headers)) {
      if (value === undefined) delete headers[key];
    }

    if (this.options.webhook === true) {
      headers = {
        'User-Agent': this.client.options.http.headers['User-Agent'],
      };
    }

    if (this.options.DiscordContext) {
      headers['X-Context-Properties'] = Buffer.from(JSON.stringify(this.options.DiscordContext), 'utf8').toString(
        'base64',
      );
    }

    if (this.options.mfaToken) {
      headers['X-Discord-Mfa-Authorization'] = this.options.mfaToken;
    }

    if (captchaKey && typeof captchaKey == 'string') headers['X-Captcha-Key'] = captchaKey;
    if (captchaRqToken && typeof captchaRqToken == 'string') headers['X-Captcha-Rqtoken'] = captchaRqToken;

    let body;
    if (this.options.files?.length) {
      body = new FormData();
      for (const [index, file] of this.options.files.entries()) {
        if (file?.file) {
          const fileData = Buffer.isBuffer(file.file)
            ? new Blob([file.file])
            : typeof file.file === 'string'
              ? new Blob([Buffer.from(file.file)])
              : {
                  [Symbol.toStringTag]: 'File',
                  name: file.name,
                  stream: () => file.file,
                };
          body.set(file.key ?? `files[${index}]`, fileData, file.name);
        }
      }
      if (typeof this.options.data !== 'undefined') {
        if (this.options.dontUsePayloadJSON) {
          for (const [key, value] of Object.entries(this.options.data)) body.append(key, value);
        } else {
          body.append('payload_json', JSON.stringify(this.options.data));
        }
      }
      // eslint-disable-next-line eqeqeq
    } else if (this.options.data != null) {
      if (this.options.usePayloadJSON) {
        body = new FormData();
        body.append('payload_json', JSON.stringify(this.options.data));
      } else {
        body = JSON.stringify(this.options.data);
        headers['Content-Type'] = 'application/json';
      }
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.client.options.restRequestTimeout).unref();

    // Deduplicate identical concurrent GET requests
    const isDeduplicable = this.method === 'get' && !body && !captchaKey;
    if (isDeduplicable) {
      const dedupeKey = `${url}:${headers.Authorization || ''}`;
      const existing = inflightGets.get(dedupeKey);
      if (existing) {
        clearTimeout(timeout);
        return existing;
      }
      const promise = this._doFetch(url, headers, body, controller, dispatcher)
        .finally(() => {
          clearTimeout(timeout);
          inflightGets.delete(dedupeKey);
        });
      inflightGets.set(dedupeKey, promise);
      return promise;
    }

    return this._doFetch(url, headers, body, controller, dispatcher)
      .finally(() => clearTimeout(timeout));
  }

  _doFetch(url, headers, body, controller, dispatcher) {
    return this.rest.fetch(url, {
      method: this.method.toUpperCase(),
      headers,
      body,
      signal: controller.signal,
      redirect: 'follow',
      dispatcher,
      credentials: 'include',
    });
  }
}

module.exports = APIRequest;
