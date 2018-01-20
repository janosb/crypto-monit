from dateutil.parser import parse
from telethon import TelegramClient
import telethon
import pandas as pd 
import re
from tg_api import *

SUCCESS = True
FAILURE = False

def get_tg_client():
	client = TelegramClient('session_name', api_id, api_hash)
	client.start()
	return client


class TgMessage(object):
	"""Telegram Message"""

	def __init__(self, channel, message):
		super(TgMessage, self).__init__()
		self.views = message.views
		self.channel = channel
		self.time = message.date
		self.message = message.message
		self.coin = ""
		self.extract_exchange()

	def extract_coin(self):
		if self.channel == "cryptovipsignall":
			status = self.extract_cryptovipsignall()
			return SUCCESS if status else FAILURE
		else:
			status = self.extract_general()
			return SUCCESS if status else FAILURE

	def extract_cryptovipsignall(self):
		m = self.message.upper()
		if "#" in m and "BUY" in m:
			coins_mentioned = [x.split("#")[1] for x in m.split() if "#" in x]
			self.coin = coins_mentioned[0].replace("/BTC","").replace("-BTC","")
			return SUCCESS
		if "/BTC" in m and "BITTREX/POLONIEX" in m:
			self.coin = m.split('/')[0]
			return SUCCESS
		return FAILURE

	def extract_general(self):
		m = self.message.upper()
		if "#" in m and (("TARGET" in m) or ("BUY" in m)):
			coins_mentioned = [x.split("#")[1] for x in m.split() if "#" in x]
			self.coin = coins_mentioned[0].replace("/BTC","").replace("-BTC","")
			return SUCCESS
		return FAILURE

	def extract_exchange(self):
		m = self.message.upper()
		if "BITTREX" in m:
			self.exchange = "BITTREX"
		elif "BINANCE" in m:
			self.exchange = "BINANCE"
		else:
			self.exchange = "UNKNOWN"

	def msg_to_dict(self):
		self.remove_emojis()
		return {"coin":self.coin,
				"exchange":self.exchange,	
				"views":self.views,
				"channel":self.channel,
				"time":self.time.isoformat(),
				"message":self.message
				}

	def remove_emojis(self):
		emoji_pattern = re.compile("["
        		u"\U0001F600-\U0001F64F"  # emoticons
        		u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        		u"\U0001F680-\U0001F6FF"  # transport & map symbols
        		u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                "]+", flags=re.UNICODE)
		self.message = emoji_pattern.sub(r'', self.message)


def get_channel_messages(client, channel, before_date):
	messages = client.get_message_history(channel, limit=None, offset_date=before_date)
	message_list = []
	print("found %d messages" % len(messages))
	for m in messages:
		if type(m) != telethon.tl.types.Message: continue

		tgm = TgMessage(channel, m)
		status = tgm.extract_coin()
		message_list.append(tgm.msg_to_dict())
	return message_list
		

