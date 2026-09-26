// A tiny web client: pair, pick an agent, chat, answer permission requests.
// Run with `pnpm example:web` next to a host (for example the fake host:
// `oal-conformance client --listen 127.0.0.1:7878`).

import { connect, pair, type Agent, type Client, type Identity, type PermissionRequest, type Session } from '@openagentlink/client';

const STORE = 'oal-example';
const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const pairForm = $<HTMLFormElement>('pair-form');
const chat = $<HTMLElement>('chat');
const log = $<HTMLDivElement>('log');
const agentSelect = $<HTMLSelectElement>('agent');
const modeSelect = $<HTMLSelectElement>('mode');
const promptForm = $<HTMLFormElement>('prompt-form');
const promptInput = $<HTMLInputElement>('prompt');
const stop = $<HTMLButtonElement>('stop');
const status = $<HTMLParagraphElement>('status');

interface Saved {
  url: string;
  identity: Identity;
}

let client: Client | undefined;
let agents: Agent[] = [];
let session: Session | undefined;
const cards = new WeakMap<PermissionRequest, HTMLElement>();

function load(): Saved | undefined {
  try {
    const raw = localStorage.getItem(STORE);
    return raw ? (JSON.parse(raw) as Saved) : undefined;
  } catch {
    return undefined;
  }
}

function line(text: string, kind = ''): HTMLDivElement {
  const div = document.createElement('div');
  div.className = kind;
  div.textContent = text;
  log.append(div);
  log.scrollTop = log.scrollHeight;
  return div;
}

function ask(request: PermissionRequest): void {
  const card = document.createElement('div');
  card.className = 'ask';
  card.append(`Allow: ${request.toolCall.title ?? 'a tool'}?`);
  const choices = document.createElement('div');
  choices.className = 'choices';
  for (const option of request.options) {
    const button = document.createElement('button');
    button.textContent = option.name;
    if (option.kind.startsWith('reject')) button.className = 'quiet';
    button.onclick = () => request.choose(option.optionId).catch((error: Error) => (status.textContent = error.message));
    choices.append(button);
  }
  card.append(choices);
  log.append(card);
  cards.set(request, card);
}

async function openSession(): Promise<void> {
  const agent = agents.find((a) => a.id === agentSelect.value);
  if (!agent) return;
  session = await agent.session();
  modeSelect.replaceChildren(
    ...(session.modes?.availableModes ?? []).map((mode) => new Option(mode.name, mode.id, false, mode.id === session!.modes!.currentModeId)),
  );
  modeSelect.hidden = !session.modes;
  log.replaceChildren();
  status.textContent = `${agent.label} (${agent.runtime}) in ${agent.folder ?? 'its workspace'}`;
}

async function start(saved: Saved): Promise<void> {
  pairForm.hidden = true;
  client = await connect({ url: saved.url, credentials: saved.identity });
  const host = client.hosts()[0]!;
  agents = await host.agents();
  agentSelect.replaceChildren(...agents.map((a) => new Option(`${a.label}${a.online ? '' : ' (offline)'}`, a.id)));
  chat.hidden = false;
  await openSession();
  void (async () => {
    for await (const update of host.updates()) {
      if (update.type === 'presence') status.textContent = update.online ? `Connected to ${host.name}.` : `Reconnecting to ${host.name}...`;
    }
  })();
}

pairForm.onsubmit = async (event) => {
  event.preventDefault();
  const form = new FormData(pairForm);
  const url = String(form.get('url'));
  try {
    const identity = await pair({ url, code: String(form.get('code')), deviceName: 'OAL web example' });
    const saved = { url, identity };
    localStorage.setItem(STORE, JSON.stringify(saved));
    await start(saved);
  } catch (error) {
    status.textContent = (error as Error).message;
  }
};

agentSelect.onchange = () => void openSession();
modeSelect.onchange = () => session?.setMode(modeSelect.value).catch((error: Error) => (status.textContent = error.message));
stop.onclick = () => void session?.cancel();
$<HTMLButtonElement>('forget').onclick = async () => {
  await client?.hosts()[0]?.unpair().catch(() => {});
  client?.close();
  localStorage.removeItem(STORE);
  location.reload();
};

promptForm.onsubmit = async (event) => {
  event.preventDefault();
  const text = promptInput.value.trim();
  if (!text || !session) return;
  promptInput.value = '';
  line(text, 'you');
  let reply: HTMLDivElement | undefined;
  stop.disabled = false;
  for await (const e of session.prompt(text)) {
    if (e.type !== 'text') reply = undefined;
    switch (e.type) {
      case 'text':
        reply ??= line('');
        reply.textContent += e.text;
        break;
      case 'thinking':
        line(e.text, 'thinking');
        break;
      case 'tool_start':
        line(`> ${e.tool.title ?? e.tool.toolCallId}`, 'tool');
        break;
      case 'tool_result':
        line(`${e.tool.status}: ${e.output}`, 'tool');
        break;
      case 'permission':
        ask(e.request);
        break;
      case 'permission_resolved': {
        const card = cards.get(e.request);
        if (card) {
          card.className = 'ask done';
          card.textContent = `${e.request.toolCall.title}: ${e.outcome?.outcome === 'selected' ? e.outcome.optionId : 'cancelled'}${e.answeredBy ? ` (${e.answeredBy.name})` : ''}`;
        }
        break;
      }
      case 'plan':
        line(e.entries.map((entry) => `[${entry.status}] ${entry.content}`).join('\n'), 'tool');
        break;
      case 'error':
        line(e.error.message, 'error');
        break;
      case 'done':
        if (e.stopReason === 'cancelled') line('Stopped.', 'tool');
        break;
    }
  }
  stop.disabled = true;
};

const saved = load();
if (saved) start(saved).catch((error: Error) => (status.textContent = error.message));
else pairForm.hidden = false;
