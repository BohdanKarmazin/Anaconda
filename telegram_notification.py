import time
import requests

counter = 0


def telegram_bot_send_message(message, silent=False):
    global counter
    bot_token = '1671415239:AAGrdXcuEHZMbrq0UUjpiIbSrEbKtsEN1fs'
    #чат
    bot_chat_id = '-1001823448052'
    #група
    # bot_chat_id = '-1002115490829'

    if silent is True:
        send_text = 'https://api.telegram.org/bot' \
                    + bot_token + '/sendMessage?chat_id=' \
                    + bot_chat_id + '&parse_mode=Markdown&text=' \
                    + message + '&disable_notification=True'
    else:
        send_text = 'https://api.telegram.org/bot' \
                    + bot_token + '/sendMessage?chat_id=' \
                    + bot_chat_id + '&parse_mode=Markdown&text=' \
                    + message
    if counter == 0:
        response = requests.get(send_text)
        counter += 1
        print(response.json())
    else:
        time.sleep(3)
        counter = 0
        telegram_bot_send_message(message, silent=False)


if __name__ == '__main__':
    msgs = ['1', '2', '3', '4', '5', '6']
    for m in msgs:
        telegram_bot_send_message('Test' + m, silent=False)
