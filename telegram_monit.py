import sys
import signal
import time
import datetime

from telethon import TelegramClient
from message_features import MessageFeatures, GeneralClassifier
from telegram_message import TgMessagem, get_tg_client
from tg_api_config import *


def signal_handler(signal, frame):
    print("\nprogram interrupted or killed")
    sys.exit(0)


class MonitoredChannel(object):
	"""docstring for MonitoredChannel"""
	def __init__(self, name, min_id):
		self.name = name
		self.min_id = min_id

	def get_min_id_from_db(self):
		print('TODO: call DB to figure out last message that was added to db')
		#self.min_id = ...

	def get_new_messages(self, client):
		messages = client.get_message_history(self.name, limit=None, min_id = self.min_id)
		if len(messages) == 0:
			return []
		print(self.name, len(messages)," messages found")
		self.min_id = messages[0].id 
		return messages


class TelegramMonitor(object):

	def __init__(self):
		self.client = self.get_tg_client()
		self.channels = self.get_monitored_channels()
		self.classifier = GeneralClassifier.initialize_from_file()
		self.run()

	def get_tg_client(self):
		client = TelegramClient('', api_id, api_hash)
		client.start()
		return client

	def run(self):
		# handle interrupts, otherwise monitor forever
		signal.signal(signal.SIGINT, signal_handler)

		while True:
			self.ping_for_messages()
			time.sleep(15)

	def get_monitored_channels(self):
		channels = []
		for channel, min_id in zip(monitored_channels, monitored_channels_min_id):
			channels.append(MonitoredChannel(channel, min_id))
		return channels


	def ping_for_messages(self):
		for channel in self.channels:
			#print('checking channel: %s' % channel.name)
			messages = channel.get_new_messages(self.client)

			for msg in messages[::-1]:
				if not isinstance(msg, telethon.tl.types.Message): continue

				mf = MessageFeatures.create_instance(msg.message)
				if mf:
					prediction = self.classifier.predict_message(mf)
					if prediction == 1:
						print("Pump and Dump happening at %s! Coin: %s" 
							% (msg.date, mf.first_coin_mentioned))
					else:
						print("did not detect pump in message: %s" % msg.date)
				tgm = TgMessage(channel.name, msg, label=prediction)
				tgm.append_to_file()

if __name__=='__main__':
	tm = TelegramMonitor()


