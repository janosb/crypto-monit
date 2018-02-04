import sys
import signal
import time
import datetime
import pandas as pd
import psycopg2
import telethon
from message_features import MessageFeatures
from telegram_message import TgMessage, get_tg_client
from tg_api_config import *
from db_connection import conn, cursor


def signal_handler(signal, frame):
    print("\nprogram interrupted or killed")
    sys.exit(0)


class MonitoredChannel(object):
	def __init__(self, name, min_id, db_index, subscribers):
		self.name = name
		self.min_id = min_id
		self.db_index = db_index
		self.subscribers = subscribers

	def get_new_messages(self, client):
		return client.get_message_history(self.name, limit=None, min_id = self.min_id)

	def update_min_id(self, new_id):
		self.min_id = new_id
		query = "UPDATE channels SET min_id = %d WHERE index = %d" % (new_id, self.db_index) 
		cursor.execute(query)
		conn.commit()


class TelegramMonitor(object):

	def __init__(self):
		self.channels = self.get_monitored_channels()
		self.client = self.get_tg_client()
		self._run()

	def get_tg_client(self):
		client = telethon.TelegramClient('', api_id, api_hash) # error handling
		client.start()
		return client

	def _run(self):
		# handle interrupts, otherwise monitor forever
		signal.signal(signal.SIGINT, signal_handler)

		while True:
			self.ping_for_messages()
			time.sleep(tg_sleep_time_sec)

	def get_monitored_channels(self):
		query = "select distinct on (channel) index, channel, min_id, subscribers from channels;"
		channels_df = pd.read_sql_query(query, conn) # error handling
		return [MonitoredChannel(row['channel'], row['min_id'], row['index'], row['subscribers']) 
					for index, row in channels_df.iterrows()]

	def ping_for_messages(self):
		for channel in self.channels:
			messages = channel.get_new_messages(self.client)
			
			for msg in messages:
				if not isinstance(msg, telethon.tl.types.Message): continue
				tgm = TgMessage(channel.name, msg.message, msg.date, msg.id, channel.subscribers)
			if messages:
				channel.update_min_id(messages[0].id)


if __name__=='__main__':
	tm = TelegramMonitor()



