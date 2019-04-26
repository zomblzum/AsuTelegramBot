import telebot, requests
from telebot.types import Message
from telebot import apihelper

from threading import Timer, Thread

from sql.sqlmanager import SqlManager, SqlAlertDeadlockAnnouncer
from utils import botinfo

bot = telebot.TeleBot(botinfo.TOKEN)
apihelper.proxy = botinfo.PROXY_INF

class Announcer(object):
    def __init__(self, interval = 60):
        self.interval = interval
        self.deadlock_announcer = SqlAlertDeadlockAnnouncer()
        self.chats = botinfo.CHAT_ID
        self.url = botinfo.MAIN_URL
        self.proxy = botinfo.PROXY_INF

    def run(self):
        Timer(self.interval, self.run).start ()
        events = []
        try:
            events.append(self.deadlock_announcer.run())
            if events.count != 0:
                for events_text in events:
                    for id in self.chats:
                        message = {'chat_id':id,'text':events_text}
                        requests.post(self.url + '/sendMessage',message,proxies=self.proxy)
        except Exception:
            print('Announcer Error:',Exception.args[0])

class AsuBot():
    def __init__(self):
        self.sqlmanager = SqlManager()
        self.deadlock_announcer = SqlAlertDeadlockAnnouncer()

    def help(self):
        return 'Здесь будет информация, но не сегодня'

    def exec_sql(self, query):
        result = self.sqlmanager.exec_query(query)
        return result

    def check_deadlocks(self):
        result = self.deadlock_announcer.check_deadlocks()
        return 'Сегодня было {} дедлоков'.format(str(result))

@bot.message_handler(commands=['help'])
def command_handler(message: Message):
    result = AsuBot().help()
    bot.send_message(message.chat.id, result)

@bot.message_handler(content_types=['text'])
@bot.edited_message_handler(content_types=['text'])
def message_handler(message: Message):
    if message.text.lower() == 'debug_test_message':
        print(message)

    if message.text.lower() == 'привет':
        bot.reply_to(message, 'Привет {}'.format(message.from_user.first_name))

    if message.text.lower().find('длоки') > -1:
        result = AsuBot().check_deadlocks()
        bot.send_message(message.chat.id, result)

    # if message.text.lower().find('sql') > -1:
    #     result = AsuBot().exec_sql(query = str(message.text).replace('sql',''))
    #     bot.send_message(message.chat.id, result)
    # if message.text.lower().find('select') > -1:
    #     result = AsuBot().exec_sql(query = message.text)
    #     bot.send_message(message.chat.id, result)

# Event announcer
anouncer_thread = Thread(Announcer(interval=60).run())
anouncer_thread.start()

# Telegram bot
bot_thread = Thread(bot.polling(30))
bot_thread.start()