
import requests
import os


# telegram
telegramToken_MarketPhases = os.environ.get('telegramToken_MarketPhases')
telegram_chat_id = os.environ.get('telegram_chat_id')

telegram_tokens = {'market_phases', 'closed_positions', '1h', '4h', '1d'}

def sendTelegramMessage(msg):
    try:

        max_limit = 4096
        additional_characters = "<pre> </pre>Part [10/99]"
        num_additional_characters = len(additional_characters)
        max_limit = 4096 - num_additional_characters 

        if len(msg+additional_characters) > max_limit:
            # Split the message into multiple parts
            message_parts = [msg[i:i+max_limit] for i in range(0, len(msg), max_limit)]
            n_parts = len(message_parts)
            for i, part in enumerate(message_parts):
                print(f'Part [{i+1}/{n_parts}]\n{part}')
                
                lmsg = "<pre>Part ["+str(i+1)+"/"+str(n_parts)+"]\n"+part+"</pre>"
                params = {
                "chat_id": telegram_chat_id,
                "text": lmsg,
                "parse_mode": "HTML",
                }
            
                resp = requests.post("https://api.telegram.org/bot{}/sendMessage".format(telegramToken_MarketPhases), params=params).json()
                
        else:
            # Message is within the maximum limit
            
            # To fix the issues with dataframes alignments, the message is sent as HTML and wraped with <pre> tag
            # Text in a <pre> element is displayed in a fixed-width font, and the text preserves both spaces and line breaks
            lmsg = "<pre>"+msg+"</pre>"

            params = {
            "chat_id": telegram_chat_id,
            "text": lmsg,
            "parse_mode": "HTML",
            }
        
            resp = requests.post("https://api.telegram.org/bot{}/sendMessage".format(telegramToken_MarketPhases), params=params).json()
    except Exception as e:
        msg = "sendTelegramMessage - There was an error: "
        print(msg, e)
        pass