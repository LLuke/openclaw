import { createLoggerBackedRuntime, type RuntimeEnv } from "openclaw/plugin-sdk/irc";
import { resolveIrcAccount } from "./accounts.js";
import { connectIrcClient, type IrcClient } from "./client.js";
import { buildIrcConnectOptions } from "./connect-options.js";
import { handleIrcInbound } from "./inbound.js";
import { isChannelTarget } from "./normalize.js";
import { makeIrcMessageId } from "./protocol.js";
import { getIrcRuntime } from "./runtime.js";
import type { CoreConfig, IrcInboundMessage } from "./types.js";

/**
 * Registry of currently active IRC monitor clients, keyed by account ID.
 * Allows probeIrc to skip a redundant TCP connection when the main monitor
 * socket is already open and healthy.
 */
const activeMonitorClients = new Map<string, IrcClient>();

export function getActiveIrcMonitorClient(accountId: string): IrcClient | null {
  return activeMonitorClients.get(accountId) ?? null;
}

export type IrcMonitorOptions = {
  accountId?: string;
  config?: CoreConfig;
  runtime?: RuntimeEnv;
  abortSignal?: AbortSignal;
  statusSink?: (patch: { lastInboundAt?: number; lastOutboundAt?: number }) => void;
  onMessage?: (message: IrcInboundMessage, client: IrcClient) => void | Promise<void>;
};

export function resolveIrcInboundTarget(params: { target: string; senderNick: string }): {
  isGroup: boolean;
  target: string;
  rawTarget: string;
} {
  const rawTarget = params.target;
  const isGroup = isChannelTarget(rawTarget);
  if (isGroup) {
    return { isGroup: true, target: rawTarget, rawTarget };
  }
  const senderNick = params.senderNick.trim();
  return { isGroup: false, target: senderNick || rawTarget, rawTarget };
}

export async function monitorIrcProvider(opts: IrcMonitorOptions): Promise<{ stop: () => void }> {
  const core = getIrcRuntime();
  const cfg = opts.config ?? (core.config.loadConfig() as CoreConfig);
  const account = resolveIrcAccount({
    cfg,
    accountId: opts.accountId,
  });

  const runtime: RuntimeEnv =
    opts.runtime ??
    createLoggerBackedRuntime({
      logger: core.logging.getChildLogger(),
      exitError: () => new Error("Runtime exit not available"),
    });

  if (!account.configured) {
    throw new Error(
      `IRC is not configured for account "${account.accountId}" (need host and nick in channels.irc).`,
    );
  }

  const logger = core.logging.getChildLogger({
    channel: "irc",
    accountId: account.accountId,
  });

  let client: IrcClient | null = null;
  let lastError: Error | null = null;

  client = await connectIrcClient(
    buildIrcConnectOptions(account, {
      channels: account.config.channels,
      abortSignal: opts.abortSignal,
      onLine: (line) => {
        if (core.logging.shouldLogVerbose()) {
          logger.debug?.(`[${account.accountId}] << ${line}`);
        }
      },
      onNotice: (text, target) => {
        if (core.logging.shouldLogVerbose()) {
          logger.debug?.(`[${account.accountId}] notice ${target ?? ""}: ${text}`);
        }
      },
      onError: (error) => {
        lastError = error;
        logger.error(`[${account.accountId}] IRC error: ${error.message}`);
      },
      onPrivmsg: async (event) => {
        if (!client) {
          return;
        }
        if (event.senderNick.toLowerCase() === client.nick.toLowerCase()) {
          return;
        }

        const inboundTarget = resolveIrcInboundTarget({
          target: event.target,
          senderNick: event.senderNick,
        });
        const message: IrcInboundMessage = {
          messageId: makeIrcMessageId(),
          target: inboundTarget.target,
          rawTarget: inboundTarget.rawTarget,
          senderNick: event.senderNick,
          senderUser: event.senderUser,
          senderHost: event.senderHost,
          text: event.text,
          timestamp: Date.now(),
          isGroup: inboundTarget.isGroup,
        };

        core.channel.activity.record({
          channel: "irc",
          accountId: account.accountId,
          direction: "inbound",
          at: message.timestamp,
        });

        if (opts.onMessage) {
          await opts.onMessage(message, client);
          return;
        }

        await handleIrcInbound({
          message,
          account,
          config: cfg,
          runtime,
          connectedNick: client.nick,
          sendReply: async (target, text) => {
            client?.sendPrivmsg(target, text);
            opts.statusSink?.({ lastOutboundAt: Date.now() });
            core.channel.activity.record({
              channel: "irc",
              accountId: account.accountId,
              direction: "outbound",
            });
          },
          statusSink: opts.statusSink,
        });
      },
    }),
  );

  logger.info(
    `[${account.accountId}] connected to ${account.host}:${account.port}${account.tls ? " (tls)" : ""} as ${client.nick}`,
  );

  // Register so probeIrc can short-circuit when the monitor is already connected.
  activeMonitorClients.set(account.accountId, client);

  // Keep this async function alive until the socket closes. Without this await,
  // the function would return immediately after connecting and server-channels
  // would treat the task as "completed", triggering spurious auto-restarts that
  // hammer the IRC server with reconnection attempts using the same nick.
  await client.closed;

  activeMonitorClients.delete(account.accountId);

  const stopped = client;
  client = null;

  if (lastError) {
    throw lastError;
  }

  return {
    stop: () => {
      // Connection is already closed; nothing to do.
      stopped?.quit("shutdown");
    },
  };
}
