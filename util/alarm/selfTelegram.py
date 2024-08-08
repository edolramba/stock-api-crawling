from common.importConfig import *
from telegram import Bot
from telegram.constants import ParseMode
import asyncio

class selfTelegram():
    importConfig = importConfig()
    token_data = importConfig.select_section("TELEGRAM")
    if token_data is None:
        print("TELEGRAM section not found in configuration.")
        token = None
        chat_id = None
    else:
        token = token_data["self_token"]
        chat_id = token_data["self_chat_id"]

    bot = Bot(token)

    async def send(self, msg):
        try:
            text = str(msg)
            if len(text) <= 4096:
                await self.bot.send_message(self.chat_id, text)
            else:
                parts = []
                while len(text) > 0:
                    if len(text) > 4080:
                        part = text[:4080]
                        first_lnbr = part.rfind('\n')
                        if first_lnbr != -1:
                            parts.append(part[:first_lnbr])
                            text = text[first_lnbr:]
                        else:
                            parts.append(part)
                            text = text[4080:]
                    else:
                        parts.append(text)
                        break
                for idx, part in enumerate(parts):
                    if idx == 0:
                        await self.bot.send_message(self.chat_id, text=part)
                    else:
                        await self.bot.send_message(self.chat_id, text='(Continuing...)\n' + part)
        except Exception as e:
            print(f"Error sending message: {e}")

    async def sendToHTML(self, msg):
        try:
            text = str(msg)
            if len(text) <= 4096:
                await self.bot.send_message(self.chat_id, text, parse_mode=ParseMode.HTML)
            else:
                parts = []
                while len(text) > 0:
                    if len(text) > 4080:
                        part = text[:4080]
                        first_lnbr = part.rfind('\n')
                        if first_lnbr != -1:
                            parts.append(part[:first_lnbr])
                            text = text[first_lnbr:]
                        else:
                            parts.append(part)
                            text = text[4080:]
                    else:
                        parts.append(text)
                        break
                for idx, part in enumerate(parts):
                    if idx == 0:
                        await self.bot.send_message(self.chat_id, text=part, parse_mode=ParseMode.HTML)
                    else:
                        await self.bot.send_message(self.chat_id, text='(Continuing...)\n' + part, parse_mode=ParseMode.HTML)
        except Exception as e:
            print(f"Error sending HTML message: {e}")

    async def sendToMARKDOWN(self, msg):
        try:
            text = str(msg)
            if len(text) <= 4096:
                await self.bot.send_message(self.chat_id, text, parse_mode=ParseMode.MARKDOWN_V2)
            else:
                parts = []
                while len(text) > 0:
                    if len(text) > 4080:
                        part = text[:4080]
                        first_lnbr = part.rfind('\n')
                        if first_lnbr != -1:
                            parts.append(part[:first_lnbr])
                            text = text[first_lnbr:]
                        else:
                            parts.append(part)
                            text = text[4080:]
                    else:
                        parts.append(text)
                        break
                for idx, part in enumerate(parts):
                    if idx == 0:
                        await self.bot.send_message(self.chat_id, text=part, parse_mode=ParseMode.MARKDOWN_V2)
                    else:
                        await self.bot.send_message(self.chat_id, text='(Continuing...)\n' + part, parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as e:
            print(f"Error sending Markdown message: {e}")

if __name__ == '__main__':
    async def main():
        telegram_bot = selfTelegram()
        await telegram_bot.send("Test message")

    asyncio.run(main())
