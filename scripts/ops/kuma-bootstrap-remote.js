#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

function usage() {
  console.error(
    [
      "Usage:",
      "  node scripts/ops/kuma-bootstrap-remote.js \\",
      "    --host lightswarm@192.168.31.15 \\",
      "    --config scripts/ops/kuma-monitors.example.json \\",
      "    --username lightswarm \\",
      "    --password '<kuma password>'",
      "",
      "Optional:",
      "  --token '<kuma jwt>'",
      "  --port 3001",
      "  --container uptime-kuma",
      "  --api-key-name codex-bootstrap",
      "  --write-api-key /tmp/kuma-api-key.txt",
    ].join("\n")
  );
}

function parseArgs(argv) {
  const args = {
    port: "3001",
    container: "uptime-kuma",
  };

  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    if (!arg.startsWith("--")) {
      throw new Error(`Unexpected argument: ${arg}`);
    }
    if (!next || next.startsWith("--")) {
      throw new Error(`Missing value for ${arg}`);
    }
    args[arg.slice(2)] = next;
    i += 1;
  }

  for (const key of ["host", "config"]) {
    if (!args[key]) {
      throw new Error(`Missing required argument --${key}`);
    }
  }

  const hasToken = Boolean(args.token);
  const hasPasswordLogin = Boolean(args.username && args.password);
  if (!hasToken && !hasPasswordLogin) {
    throw new Error("Provide either --token or both --username and --password");
  }

  return args;
}

function buildRemoteScript() {
  return String.raw`
const { io } = require("socket.io-client");

function emitAsync(socket, event, payload) {
  return new Promise((resolve, reject) => {
    socket.emit(event, payload, (response) => {
      if (!response) {
        reject(new Error(event + " returned no response"));
        return;
      }
      if (response.ok === false) {
        reject(new Error(response.msg || (event + " failed")));
        return;
      }
      resolve(response);
    });
  });
}

function emitVariadicAsync(socket, event, args) {
  return new Promise((resolve, reject) => {
    socket.emit(event, ...args, (response) => {
      if (!response) {
        reject(new Error(event + " returned no response"));
        return;
      }
      if (response.ok === false) {
        reject(new Error(response.msg || (event + " failed")));
        return;
      }
      resolve(response);
    });
  });
}

function onceWithTimeout(socket, event, timeoutMs) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      socket.off(event, onEvent);
      reject(new Error("Timed out waiting for " + event));
    }, timeoutMs);

    const onEvent = (payload) => {
      clearTimeout(timeout);
      resolve(payload);
    };

    socket.once(event, onEvent);
  });
}

function normalizeMonitor(input) {
  const base = {
    name: input.name,
    type: input.type || "http",
    active: input.active !== false,
    interval: input.interval ?? 60,
    retryInterval: input.retryInterval ?? 60,
    resendInterval: input.resendInterval ?? 0,
    timeout: input.timeout ?? 48,
    maxretries: input.maxretries ?? 0,
    upsideDown: input.upsideDown === true,
    ignoreTls: input.ignoreTls === true,
    maxredirects: input.maxredirects ?? 10,
    accepted_statuscodes: input.accepted_statuscodes || ["200-299"],
    method: input.method || "GET",
    body: input.body || null,
    headers: input.headers
      ? typeof input.headers === "string"
        ? input.headers
        : JSON.stringify(input.headers)
      : null,
    basic_auth_user: input.basic_auth_user || null,
    basic_auth_pass: input.basic_auth_pass || null,
    authMethod: input.authMethod || null,
    authDomain: input.authDomain || null,
    authWorkstation: input.authWorkstation || null,
    packetSize: input.packetSize ?? 56,
    mqttTopic: input.mqttTopic || "",
    mqttSuccessMessage: input.mqttSuccessMessage || "",
    mqttUsername: input.mqttUsername || "",
    mqttPassword: input.mqttPassword || "",
    databaseConnectionString: input.databaseConnectionString || "",
    databaseQuery: input.databaseQuery || "",
    grpcUrl: input.grpcUrl || null,
    grpcProtobuf: input.grpcProtobuf || null,
    grpcBody: input.grpcBody || null,
    grpcMetadata: input.grpcMetadata || null,
    grpcMethod: input.grpcMethod || null,
    grpcServiceName: input.grpcServiceName || null,
    grpcEnableTls: input.grpcEnableTls === true,
    radiusUsername: input.radiusUsername || "",
    radiusPassword: input.radiusPassword || "",
    radiusCallingStationId: input.radiusCallingStationId || "",
    radiusCalledStationId: input.radiusCalledStationId || "",
    radiusSecret: input.radiusSecret || "",
    httpBodyEncoding: input.httpBodyEncoding || null,
    keyword: input.keyword || "",
    invertKeyword: input.invertKeyword === true,
    jsonPath: input.jsonPath || "",
    expectedValue: input.expectedValue || "",
    kafkaProducerTopic: input.kafkaProducerTopic || "",
    kafkaProducerBrokers: input.kafkaProducerBrokers || [],
    kafkaProducerSaslOptions: input.kafkaProducerSaslOptions || {},
    kafkaProducerMessage: input.kafkaProducerMessage || "",
    kafkaProducerSsl: input.kafkaProducerSsl === true,
    kafkaProducerAllowAutoTopicCreation: input.kafkaProducerAllowAutoTopicCreation === true,
    notificationIDList: input.notificationIDList || {},
    parent: input.parent ?? null,
    description: input.description || null,
  };

  if (base.type === "http" || base.type === "keyword" || base.type === "json-query") {
    base.url = input.url;
    if (!base.url) {
      throw new Error("HTTP-like monitor requires url: " + input.name);
    }
  }

  if (base.type === "keyword" && !base.keyword) {
    throw new Error("Keyword monitor requires keyword: " + input.name);
  }

  if (base.type === "json-query" && !base.jsonPath) {
    throw new Error("JSON query monitor requires jsonPath: " + input.name);
  }

  if (base.type === "port" || base.type === "ping") {
    base.hostname = input.hostname;
    if (!base.hostname) {
      throw new Error("Port/ping monitor requires hostname: " + input.name);
    }
  }

  if (base.type === "port") {
    base.port = input.port;
    if (!base.port) {
      throw new Error("Port monitor requires port: " + input.name);
    }
  }

  if (input.hostname) {
    base.hostname = input.hostname;
  }

  if (input.port) {
    base.port = input.port;
  }

  if (Array.isArray(input.tags)) {
    base.tags = input.tags;
  }

  return base;
}

async function main() {
  const config = JSON.parse(Buffer.from(process.env.KUMA_CONFIG_B64, "base64").toString("utf8"));
  const username = process.env.KUMA_USERNAME;
  const password = process.env.KUMA_PASSWORD;
  const token = process.env.KUMA_TOKEN;
  const apiKeyName = process.env.KUMA_API_KEY_NAME || "";
  const serverUrl = process.env.KUMA_SERVER_URL || "http://127.0.0.1:3001";

  if (!Array.isArray(config.monitors)) {
    throw new Error("Config must contain a monitors array");
  }

  const socket = io(serverUrl, {
    transports: ["websocket"],
    reconnection: false,
  });

  await new Promise((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("connect_error", reject);
  });

  let loginResponse;
  if (token) {
    loginResponse = await emitVariadicAsync(socket, "loginByToken", [token]);
  } else {
    loginResponse = await emitAsync(socket, "login", { username, password });
  }

  if (!loginResponse.ok) {
    throw new Error(loginResponse.msg || "Login failed");
  }

  const initialMonitorListPromise = onceWithTimeout(socket, "monitorList", 5000);
  await emitVariadicAsync(socket, "getMonitorList", []);
  const monitorList = await initialMonitorListPromise;

  const existingByName = new Map(
    Object.values(monitorList || {}).map((monitor) => [monitor.name, monitor])
  );

  const results = [];

  for (const rawMonitor of config.monitors) {
    const monitor = normalizeMonitor(rawMonitor);
    const existing = existingByName.get(monitor.name);

    if (existing) {
      const payload = { ...monitor, id: existing.id };
      const response = await emitAsync(socket, "editMonitor", payload);
      results.push({ action: "updated", name: monitor.name, monitorID: existing.id, response });
    } else {
      const response = await emitAsync(socket, "add", monitor);
      results.push({ action: "created", name: monitor.name, monitorID: response.monitorID, response });
    }
  }

  let apiKey = null;
  if (apiKeyName) {
    const response = await emitAsync(socket, "addAPIKey", {
      name: apiKeyName,
      active: 1,
    });
    apiKey = {
      name: apiKeyName,
      key: response.key,
      keyID: response.keyID,
    };
  }

  socket.disconnect();

  process.stdout.write(
    JSON.stringify(
      {
        ok: true,
        serverUrl,
        results,
        apiKey,
      },
      null,
      2
    ) + "\n"
  );
}

main().catch((error) => {
  console.error(JSON.stringify({ ok: false, error: error.message }, null, 2));
  process.exit(1);
});
`;
}

async function main() {
  let args;
  try {
    args = parseArgs(process.argv);
  } catch (error) {
    console.error(error.message);
    usage();
    process.exit(1);
  }

  const configPath = path.resolve(args.config);
  const configRaw = fs.readFileSync(configPath, "utf8");
  JSON.parse(configRaw);

  const remoteScript = buildRemoteScript();
  const remoteCommand = [
    "docker",
    "exec",
    "-i",
    "-e",
    `KUMA_CONFIG_B64=${Buffer.from(configRaw, "utf8").toString("base64")}`,
    "-e",
    `KUMA_SERVER_URL=http://127.0.0.1:${args.port}`,
  ];

  if (args.token) {
    remoteCommand.push("-e", `KUMA_TOKEN=${args.token}`);
  } else {
    remoteCommand.push("-e", `KUMA_USERNAME=${args.username}`);
    remoteCommand.push("-e", `KUMA_PASSWORD=${args.password}`);
  }

  if (args["api-key-name"]) {
    remoteCommand.push("-e", `KUMA_API_KEY_NAME=${args["api-key-name"]}`);
  }

  remoteCommand.push(args.container, "node", "-");

  const child = spawn("ssh", [args.host, ...remoteCommand], {
    stdio: ["pipe", "pipe", "pipe"],
  });

  let stdout = "";
  let stderr = "";

  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString();
  });

  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  child.stdin.write(remoteScript);
  child.stdin.end();

  const exitCode = await new Promise((resolve) => {
    child.on("close", resolve);
  });

  if (stderr.trim()) {
    process.stderr.write(stderr);
  }

  if (exitCode !== 0) {
    process.exit(exitCode);
  }

  process.stdout.write(stdout);

  if (args["write-api-key"]) {
    const parsed = JSON.parse(stdout);
    if (parsed.apiKey && parsed.apiKey.key) {
      fs.writeFileSync(path.resolve(args["write-api-key"]), parsed.apiKey.key + "\n", "utf8");
    }
  }
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
