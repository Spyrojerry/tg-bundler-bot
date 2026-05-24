import { createLogger } from './logger';
import { MonitorSampleEvent, ServiceConfig, TokenSummary } from './types';

const log = createLogger('TG');

export interface TelegramReply {
  text: string;
  replyMarkup?: InlineKeyboardMarkup;
  editCurrent?: boolean;
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
    await this.sendDefault(this.formatSummaryCard(summary), { pin: true });
  }

  async sendDefault(text: string, options: { pin?: boolean } = {}): Promise<void> {
    if (!this.defaultChatId) return;
    const message = await this.sendMessage(this.defaultChatId, text);
    if (options.pin) {
      await this.pinMessage(this.defaultChatId, message.message_id).catch((err) =>
        log.warn('Telegram summary pin failed', this.describeError(err))
      );
    }
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

          const reply = await this.commandHandler(chatIdString, text);
          await this.sendReply(chatIdString, reply);
        }
      } catch (err) {
        log.warn('Telegram polling error', this.describeError(err));
        await new Promise((resolve) => setTimeout(resolve, 2_000));
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
      await this.editMessage(chatId, messageId, reply.text, reply.replyMarkup);
      return;
    }
    await this.sendMessage(chatId, reply.text, reply.replyMarkup);
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
  ): Promise<void> {
    await this.api('editMessageText', {
      chat_id: chatId,
      message_id: messageId,
      text,
      parse_mode: 'HTML',
      disable_web_page_preview: true,
      ...(replyMarkup ? { reply_markup: replyMarkup } : {}),
    });
  }

  private async pinMessage(chatId: string, messageId: number): Promise<void> {
    await this.api('pinChatMessage', {
      chat_id: chatId,
      message_id: messageId,
      disable_notification: true,
    });
  }

  private async api<T = unknown>(method: string, body: Record<string, unknown>): Promise<T> {
    const resp = await fetch(`https://api.telegram.org/bot${this.token}/${method}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
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
