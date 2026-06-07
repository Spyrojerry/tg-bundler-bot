import { createLogger } from './logger';
import {
  FilterFailEvent,
  FilterPassEvent,
  MonitorSampleEvent,
  ServiceConfig,
  TokenSummary,
} from './types';

const log = createLogger('TG');

export interface TelegramReply {
  text: string;
  replyMarkup?: InlineKeyboardMarkup;
  editCurrent?: boolean;
  trackPrompt?: boolean;
}

export interface InlineKeyboardMarkup {
  inline_keyboard: InlineKeyboardButton[][];
}

interface InlineKeyboardButton {
  text: string;
  callback_data: string;
}

type CommandHandler = (chatId: string, text: string) => Promise<string | TelegramReply>;

interface TelegramUpdate {
  update_id: number;
  message?: {
    chat: { id: number | string };
    text?: string;
  };
  callback_query?: {
    id: string;
    data?: string;
    message?: {
      message_id: number;
      chat: { id: number | string };
    };
  };
}

interface TelegramMessageResponse {
  message_id: number;
}

export class TelegramBot {
  private readonly token: string;
  private readonly defaultChatId: string | null;
  private readonly commandHandler: CommandHandler;
  private readonly promptMessagesByChat = new Map<string, number>();
  private offset = 0;
  private running = false;

  constructor(config: ServiceConfig, commandHandler: CommandHandler) {
    if (!config.telegramBotToken) {
      throw new Error('TELEGRAM_BOT_TOKEN is required to start TelegramBot');
    }
    this.token = config.telegramBotToken;
    this.defaultChatId = config.telegramChatId;
    this.commandHandler = commandHandler;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    log.info('Telegram bot polling started');
    void this.bootstrapAndPoll();
  }

  stop(): void {
    this.running = false;
    log.info('Telegram bot stopped');
  }

  async sendSampleCard(event: MonitorSampleEvent): Promise<void> {
    await this.sendDefault(this.formatSampleCard(event));
  }

  async sendSummaryCard(summary: TokenSummary): Promise<void> {
    await this.sendDefault(this.formatSummaryCard(summary));
  }

  async sendFilterFailCard(
    event: FilterFailEvent,
    replyMarkup?: InlineKeyboardMarkup
  ): Promise<void> {
    await this.sendDefault(this.formatFilterFailCard(event), {
      replyMarkup,
    });
  }

  async sendFilterPassCard(event: FilterPassEvent): Promise<void> {
    await this.sendDefault(this.formatFilterPassCard(event));
  }

  async sendDefault(
    text: string,
    options: { pin?: boolean; replyMarkup?: InlineKeyboardMarkup } = {}
  ): Promise<TelegramMessageResponse | null> {
    if (!this.defaultChatId) return null;
    const message = await this.sendMessage(this.defaultChatId, text, options.replyMarkup);
    if (options.pin) {
      await this.pinMessage(this.defaultChatId, message.message_id).catch((err) =>
        log.warn('Telegram summary pin failed', this.describeError(err))
      );
    }
    return message;
  }

  async sendChat(
    chatId: string,
    text: string,
    replyMarkup?: InlineKeyboardMarkup
  ): Promise<void> {
    await this.sendMessage(chatId, text, replyMarkup);
  }

  private async bootstrapAndPoll(): Promise<void> {
    try {
      await this.api('deleteWebhook', { drop_pending_updates: false });
    } catch (err) {
      log.warn('Telegram deleteWebhook failed', this.describeError(err));
    }
    await this.pollLoop();
  }

  private async pollLoop(): Promise<void> {
    while (this.running) {
      try {
        const updates = await this.api<TelegramUpdate[]>('getUpdates', {
          offset: this.offset,
          timeout: 25,
          allowed_updates: ['message', 'callback_query'],
        });

        for (const update of updates) {
          this.offset = update.update_id + 1;
          const callbackQuery = update.callback_query;
          if (callbackQuery) {
            const chatId = callbackQuery.message?.chat.id;
            const data = callbackQuery.data?.trim();
            if (!data || chatId === undefined) continue;

            const chatIdString = String(chatId);
            if (this.defaultChatId && chatIdString !== this.defaultChatId) {
              await this.answerCallbackQuery(callbackQuery.id, 'Unauthorized chat.');
              continue;
            }

            const reply = await this.commandHandler(chatIdString, `/callback ${data}`);
            await this.answerCallbackQuery(callbackQuery.id);
            await this.sendReply(chatIdString, reply, callbackQuery.message?.message_id);
            continue;
          }

          const text = update.message?.text?.trim();
          const chatId = update.message?.chat.id;
          if (!text || chatId === undefined) continue;

          const chatIdString = String(chatId);
          if (this.defaultChatId && chatIdString !== this.defaultChatId) {
            await this.sendMessage(chatIdString, 'Unauthorized chat.');
            continue;
          }

          const promptMessageId = this.promptMessagesByChat.get(chatIdString);
          if (promptMessageId !== undefined) {
            this.promptMessagesByChat.delete(chatIdString);
            await this.deleteMessage(chatIdString, promptMessageId).catch((err) =>
              log.debug('Telegram prompt delete failed', this.describeError(err))
            );
          }

          const reply = await this.commandHandler(chatIdString, text);
          await this.sendReply(chatIdString, reply);
        }
      } catch (err) {
        const desc = this.describeError(err);
        if (desc.message === 'fetch failed') {
          log.warn('Telegram polling: network connection failed, retrying in 5s...');
          await new Promise((resolve) => setTimeout(resolve, 5_000));
        } else {
          log.warn('Telegram polling error', desc);
          await new Promise((resolve) => setTimeout(resolve, 2_000));
        }
      }
    }
  }

  private async sendReply(
    chatId: string,
    reply: string | TelegramReply,
    messageId?: number
  ): Promise<void> {
    if (typeof reply === 'string') {
      await this.sendMessage(chatId, reply);
      return;
    }
    if (reply.editCurrent && messageId !== undefined) {
      const edited = await this.editMessage(chatId, messageId, reply.text, reply.replyMarkup);
      if (reply.trackPrompt) {
        this.promptMessagesByChat.set(chatId, messageId);
      }
      if (!edited) return;
      return;
    }
    const sent = await this.sendMessage(chatId, reply.text, reply.replyMarkup);
    if (reply.trackPrompt) {
      this.promptMessagesByChat.set(chatId, sent.message_id);
    }
  }

  private async sendMessage(
    chatId: string,
    text: string,
    replyMarkup?: InlineKeyboardMarkup
  ): Promise<TelegramMessageResponse> {
    return await this.api<TelegramMessageResponse>('sendMessage', {
      chat_id: chatId,
      text,
      parse_mode: 'HTML',
      disable_web_page_preview: true,
      ...(replyMarkup ? { reply_markup: replyMarkup } : {}),
    });
  }

  private async answerCallbackQuery(callbackQueryId: string, text?: string): Promise<void> {
    await this.api('answerCallbackQuery', {
      callback_query_id: callbackQueryId,
      ...(text ? { text } : {}),
    });
  }

  private async editMessage(
    chatId: string,
    messageId: number,
    text: string,
    replyMarkup?: InlineKeyboardMarkup
  ): Promise<boolean> {
    try {
      await this.api('editMessageText', {
        chat_id: chatId,
        message_id: messageId,
        text,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
        ...(replyMarkup ? { reply_markup: replyMarkup } : {}),
      });
      return true;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (message.includes('message is not modified')) {
        return false;
      }
      throw err;
    }
  }

  public async pinMessage(chatId: string, messageId: number): Promise<void> {
    await this.api('pinChatMessage', {
      chat_id: chatId,
      message_id: messageId,
      disable_notification: true,
    });
  }

  private async deleteMessage(chatId: string, messageId: number): Promise<void> {
    await this.api('deleteMessage', {
      chat_id: chatId,
      message_id: messageId,
    });
  }

  private async api<T = unknown>(method: string, body: Record<string, unknown>): Promise<T> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 35_000); // 35s timeout (must be > getUpdates timeout of 25s)

    try {
      const resp = await fetch(`https://api.telegram.org/bot${this.token}/${method}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        signal: controller.signal,
      });

      const json = await resp.json() as {
        ok: boolean;
        result?: T;
        description?: string;
        error_code?: number;
      };
      if (!json.ok) {
        throw new Error(
          `Telegram ${method} failed (${json.error_code ?? resp.status}): ` +
          `${json.description ?? 'unknown error'}`
        );
      }
      return json.result as T;
    } finally {
      clearTimeout(timer);
    }
  }

  private describeError(err: unknown): Record<string, unknown> {
    if (err instanceof Error) {
      return {
        name: err.name,
        message: err.message,
        stack: err.stack?.split('\n').slice(0, 2).join('\n'),
      };
    }
    return { error: String(err) };
  }

  private formatSampleCard(event: MonitorSampleEvent): string {
    const metrics = event.metrics;
    return [
      `<b>${this.escapeHtml(this.short(event.walletAddress))} / ${this.escapeHtml(this.short(metrics.mint))}</b>`,
      `Wallet: <code>${this.escapeHtml(this.short(event.walletAddress))}</code>`,
      `Token: <code>${this.escapeHtml(metrics.mint)}</code>`,
      `Sample: #${event.sampleNumber} at +${event.elapsedSec}s`,
      `Bundlers: <b>${this.fmt(metrics.bundlersPercent, '%')}</b>`,
      `Bundler wallets: <b>${this.fmt(metrics.bundlersCount)}</b>`,
      `Top wallets: <b>${this.fmt(metrics.topWallets)}</b>`,
      `Matched wallets: <b>${event.matchingWallets.length}</b>`,
      `Time: ${metrics.timestamp}`,
    ].join('\n');
  }

  private formatSummaryCard(summary: TokenSummary): string {
    const windowSec = Math.round(summary.windowMs / 1_000);
    return [
      '<b>Monitoring Summary</b>',
      `Wallet: <code>${this.escapeHtml(this.short(summary.walletAddress))}</code>`,
      `Token: <code>${this.escapeHtml(summary.mint)}</code>`,
      `Window: ${windowSec}s`,
      `Samples: ${summary.totalSamples}`,
      `Period: ${summary.firstSeen} -> ${summary.lastSeen}`,
      '',
      '<b>Bundlers %</b>',
      `First: ${this.fmt(summary.bundlersPercent.first, '%')} | Last: ${this.fmt(summary.bundlersPercent.last, '%')}`,
      `Min: ${this.fmt(summary.bundlersPercent.min, '%')} | Max: ${this.fmt(summary.bundlersPercent.max, '%')}`,
      '',
      '<b>Bundler Wallets</b>',
      `First: ${this.fmt(summary.bundlersCount.first)} | Last: ${this.fmt(summary.bundlersCount.last)}`,
      `Min: ${this.fmt(summary.bundlersCount.min)} | Max: ${this.fmt(summary.bundlersCount.max)}`,
      '',
      '<b>Top Wallets</b>',
      `First: ${this.fmt(summary.topWallets.first)} | Last: ${this.fmt(summary.topWallets.last)}`,
      `Min: ${this.fmt(summary.topWallets.min)} | Max: ${this.fmt(summary.topWallets.max)}`,
      '',
      '<i>Filter note: normal % filters ignore samples below 1%.</i>',
    ].join('\n');
  }

  private formatFilterFailCard(event: FilterFailEvent): string {
    return [
      '<b>Filter Failed</b>',
      `Wallet: <code>${this.escapeHtml(this.short(event.walletAddress))}</code>`,
      `Token: <code>${this.escapeHtml(event.mint)}</code>`,
      `Sample: #${event.sampleNumber} at +${event.elapsedSec}s`,
      `Bundlers: <b>${this.fmt(event.metrics.bundlersPercent, '%')}</b>`,
      `Bundler wallets: <b>${this.fmt(event.metrics.bundlersCount)}</b>`,
      `Top wallets: <b>${this.fmt(event.metrics.topWallets)}</b>`,
      `Matched wallets: <b>${event.matchingWallets.length}</b>`,
      '',
      '<b>Reasons</b>',
      ...event.reasons.map((reason) => `- ${this.escapeHtml(reason)}`),
      '',
      'Sell submission started automatically with the configured sell settings.',
    ].join('\n');
  }

  private formatFilterPassCard(event: FilterPassEvent): string {
    return [
      '<b>Filter Passed</b>',
      `Wallet: <code>${this.escapeHtml(this.short(event.walletAddress))}</code>`,
      `Token: <code>${this.escapeHtml(event.mint)}</code>`,
      `Sample: #${event.sampleNumber} at +${event.elapsedSec}s`,
      `Bundlers: <b>${this.fmt(event.metrics.bundlersPercent, '%')}</b>`,
      `Bundler wallets: <b>${this.fmt(event.metrics.bundlersCount)}</b>`,
      `Top wallets: <b>${this.fmt(event.metrics.topWallets)}</b>`,
      `Matched wallets: <b>${event.matchingWallets.length}</b>`,
      '',
      'Decision: position left open.',
    ].join('\n');
  }

  private fmt(v: number | null, suffix = ''): string {
    return v === null ? 'N/A' : `${v}${suffix}`;
  }

  private short(value: string): string {
    return `${value.slice(0, 4)}...${value.slice(-4)}`;
  }

  escapeHtml(value: string): string {
    return value
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }
}
