import telethon
import hashlib
import pandas as pd 
import re
import os, sys

from dateutil.parser import parse
from telethon import TelegramClient
from tg_api_config import *
from message_features import MessageFeatures
from price_features import PriceFeatures
from coin_api_request import PriceDataRequest
from general_classifier import GeneralClassifier

message_classifier = GeneralClassifier.initialize_from_file()

def get_tg_client():
	client = TelegramClient('session_name', api_id, api_hash)
	client.start()
	return client


class TgMessage(object):
	"""
		Telegram Message: Parse and process message coming from telegram
	"""

	def __init__(self, channel, raw_message, time_dt, message_id, label=None, n_subscribers=None):
		self.channel = channel
		self.n_subscribers = n_subscribers  # TODO: gather these from tg client
		self.msg_label=label

		self.time_dt = time_dt  # datetime, GMT
		self.time_str = time_dt.isoformat()  
		self.raw_message = raw_message
		self.msg_id = message_id

		self.msg_hash = None
		self.symbol_id = None
		self.msg_features = None
		self.price_features = None
		self.first_coin_mentioned = None
		self.first_exchange_mentioned = None
		self.features = {}
		self.features_df = None

		# use this flag to indicate whether to use the instance or not for classification
		self.complete = False
		self.run()


	@classmethod
	def parse_message(cls, message):
		if not isinstance(msg, telethon.tl.types.Message):
			print('Not a telethon Message type')
			return

		raw_message = message.message
		time_dt = message.date  # datetime, GMT
		msg_id = message.id
		channel = message.to.username
		return cls(channel, raw_message, time_dt, msg_id)

	@classmethod
	def from_dict(cls, msg_dict):
		if not isinstance(msg_dict, dict):
			raise TypeError('Not a dict type')

		raw_message = msg_dict['raw_message']
		time_dt = parse(msg_dict['time'])
		msg_id = msg_dict['msg_id']
		channel = msg_dict['channel']
		label = msg_dict['label']
		n_subscribers = msg_dict['n_subscribers']

		return cls(channel, raw_message, time_dt, msg_id, label=label, n_subscribers=n_subscribers)

	def calculate_hash(self):
		hash_str = ':'.join([self.channel, str(self.msg_id)])
		self.msg_hash = hashlib.sha256(hash_str.encode()).hexdigest()

	def msg_to_dict(self):
		return {
				'message_hash':self.msg_hash,
				'channel':self.channel,
				'time':self.time_str,
				'message_id':self.msg_id,
				'n_subscribers':self.n_subscribers,
				'raw_message':self.raw_message
				}

	def get_feature_names(self):
		return self.features.keys()

	def is_already_saved(self):
		if not os.path.isfile(tg_message_file):
			return False

		if self.msg_hash in open(tg_message_file).read():
			print('found hash in csv: %s' % self.msg_hash)
			return True

		return False

	def append_to_csv(self, dict_to_save, filename):
		header = False if os.path.isfile(filename) else True
		df = pd.DataFrame([dict_to_save])
		with open(filename, 'a') as f:
			df.to_csv(f, index=False, header=header)

	def remove_emojis(self):
		emoji_pattern = re.compile('['
        		u'\U0001F600-\U0001F64F'  # emoticons
        		u'\U0001F300-\U0001F5FF'  # symbols & pictographs
        		u'\U0001F680-\U0001F6FF'  # transport & map symbols
        		u'\U0001F1E0-\U0001F1FF'  # flags (iOS)
                ']+', flags=re.UNICODE)
		self.raw_message = emoji_pattern.sub(r' ', self.raw_message)

	def get_time_of_day(self):
		h = self.time_dt.hour
		if h < 6:
			return 1
		elif h < 12:
			return 2
		elif h < 18:
			return 3
		else:
			return 4

	def get_coin_symbol_id(self):
		self.symbol_id = '_'.join([self.first_exchange_mentioned, 'SPOT', 
								  self.first_coin_mentioned , 'BTC']) # TODO account for -USDT markets
		print(self.symbol_id)

	def run(self):
		self.calculate_hash()
		self.remove_emojis()

		if not self.is_already_saved():
			self.append_to_csv(self.msg_to_dict(), tg_message_file)

		tod = self.get_time_of_day()
		dow = self.time_dt.weekday()

		self.message_features = MessageFeatures(self.raw_message)

		if not self.message_features.first_coin_mentioned:
			return 
		else:
			self.first_coin_mentioned = self.message_features.first_coin_mentioned

		if not self.message_features.first_exchange_mentioned:
			self.first_exchange_mentioned = 'BITTREX'  # TODO might want to search multiple exchanges here
		else:
			self.first_exchange_mentioned = self.message_features.first_exchange_mentioned

		self.get_coin_symbol_id()
		try:
			price_data_request = PriceDataRequest(self.symbol_id, self.time_dt, self.msg_hash)
		except Exception as e:
			print(self.symbol_id, e)
			return
		self.price_features = PriceFeatures(price_data_request.dataframe, self.msg_hash)

		# concatenate and save features 
		self.features = self.message_features.features
		predict_list = [self.message_features.features[k] for k in message_classifier.feature_names]
		self.features['message_label'] = message_classifier.model.predict([predict_list])[0]
		print("label:", self.features['message_label'], self.raw_message[0:20])
		self.features['time_of_day'] = tod
		self.features['day_of_week'] = dow
		for k in self.price_features.features.keys():
			self.features[k] = self.price_features.features[k]
		self.append_to_csv(self.features, tg_features_file)


		# save label
		self.price_label = self.price_features.label
		label_dict = {'msg_hash':self.msg_hash, 'label':self.price_label}
		self.append_to_csv(label_dict, tg_labels_file)

		self.complete = True




def get_channel_messages(client, channel, before_date=None, min_id=None):
	messages = client.get_message_history(channel, limit=None, offset_date=before_date, min_id=min_id)
	return messages
	
def get_channel_n_users(client, channel):
	from telethon.tl.functions.channels import GetParticipantsRequest
	from telethon.tl.types import ChannelParticipantsSearch
	from time import sleep

	offset = 0
	limit = 100

	while True:
		# TODO need channel to be of InputChannel type
		participants = client(GetParticipantsRequest(
			channel, ChannelParticipantsSearch(''), offset, limit, hash=0))
		offset += len(participants.users)
		if not participants.users:
			print('found %d users for %s' % (offset, channel))
			break
	return offset



if __name__=='__main__':
	channel = 'crypto_experts_signal'
	client = get_tg_client()
	#messages = get_channel_messages(client, 'crypto_experts_signal', min_id = 6000)[::-1]
	messages = get_channel_messages(client, 'cryptovipsignall', min_id = 0)[::-1]
	#channel = client.get_entity('https://t.me/crypto_experts_signal')
	#ns = get_channel_n_users(client, channel)
	#print(channel.participants_count)
	print(len(messages))
	features = []
	labels = []
	for msg in messages:
		tgm = TgMessage.parse_message(msg)
		if tgm and tgm.complete:
			features.append(tgm.features)
			labels.append(tgm.price_label)


		


