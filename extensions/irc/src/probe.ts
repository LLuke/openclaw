import { resolveIrcAccount } from "./accounts.js";
import { connectIrcClient } from "./client.js";
import { buildIrcConnectOptions } from "./connect-options.js";
import { getActiveIrcMonitorClient } from "./monitor.js";
import type { CoreConfig, IrcProbe } from "./types.js";

function formatError(err: unknown): string {
  if (err instanceof Error) {
    return err.message;
  }
  return typeof err === "string" ? err : JSON.stringify(err);
}

export async function probeIrc(
  cfg: CoreConfig,
  opts?: { accountId?: string; timeoutMs?: number },
): Promise<IrcProbe> {
  const account = resolveIrcAccount({ cfg, accountId: opts?.accountId });
  const base: IrcProbe = {
    ok: false,
    host: account.host,
    port: account.port,
    tls: account.tls,
    nick: account.nick,
  };

  if (!account.configured) {
    return {
      ...base,
      error: "missing host or nick",
    };
  }

  // If the gateway monitor is already connected for this account, the IRC server
  // is clearly reachable — skip the redundant TCP probe entirely. This avoids
  // spamming the IRC server with connection attempts that immediately fail with
  // 433 (nick in use) every time the status poller runs.
  const activeClient = getActiveIrcMonitorClient(account.accountId);
  if (activeClient?.isReady()) {
    return { ...base, ok: true };
  }

  const started = Date.now();
  try {
    const client = await connectIrcClient(
      buildIrcConnectOptions(account, {
        connectTimeoutMs: opts?.timeoutMs ?? 8000,
        // Skip nick fallback: a 433 means the server is live (our main connection
        // is already holding the nick), so we bail out fast without creating a
        // second long-lived connection under the fallback nick.
        noNickFallback: true,
      }),
    );
    const elapsed = Date.now() - started;
    client.quit("probe");
    return {
      ...base,
      ok: true,
      latencyMs: elapsed,
    };
  } catch (err) {
    const msg = formatError(err);
    // 433/436 = nick in use: the server responded correctly, so it is live.
    // This happens when the gateway is already connected under the configured nick.
    if (/IRC login failed \(43[36]\)/.test(msg)) {
      return {
        ...base,
        ok: true,
        latencyMs: Date.now() - started,
      };
    }
    return {
      ...base,
      error: msg,
    };
  }
}
