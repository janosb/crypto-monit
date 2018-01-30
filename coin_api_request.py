import pandas as pd 
import requests
import datetime
import hashlib
import os, sys
import json

from dateutil.parser import parse
from tg_api_config import tg_cleaned_file
from coin_api_config import *


try:
	price_df = pd.read_csv(output_price_csv, parse_dates=['time_tg_message','time_close','time_open',
											'time_period_start','time_period_end'])
	saved_price_hashes = price_df.msg_hash.unique()
except FileNotFoundError as e:
	print(e)
	price_df = None
	saved_price_hashes = []


def get_tg_df():
	return pd.read_csv(tg_cleaned_file, parse_dates=['time'], parser=parse)

def get_fake_request_data():
	return open(fake_request_data, 'r').read()


class CoinAPIDataRequest(object):

	def __init__(self, symbol_id, timestamp, msg_hash):
		self.symbol_id = symbol_id
		self.msg_hash = msg_hash

		if isinstance(timestamp, str):
			self.timestamp = parse(timestamp)
		elif isinstance(timestamp, datetime.datetime):
			self.timestamp = timestamp
		else:
			raise TypeError('Unrecognized timestamp type: %s', timestamp)

		self.minutes_before = -45
		self.minutes_after = 45
		self.time_start_iso = self.offset_timestamp(self.minutes_before)
		self.time_end_iso = self.offset_timestamp(self.minutes_after)
		self.request = None
		self.dataframe = None

	def offset_timestamp(self, dt):
			return (self.timestamp + datetime.timedelta(minutes = dt)).isoformat()

	def set_minutes_before(self, dt):
		if dt >=0:
			raise ValueError('dt must be negative')
		else:
			self.minutes_before = dt
			self.time_start_iso = self.offset_timestamp(dt)

	def set_minutes_after(self, dt):
		if dt <=0:
			raise ValueError('dt must be positive')
		else:
			self.minutes_after = dt
			self.time_end_iso = self.offset_timestamp(dt)

	def append_data_to_csv(self):
		header = False if os.path.isfile(self.outfile) else True

		with open(self.outfile, 'a') as f:
			self.dataframe.to_csv(f, header = header, index=False)

	def is_already_saved(self):
		if not os.path.isfile(self.outfile):
			return False

		if self.msg_hash in saved_price_hashes:
			return True

		return False

	def run(self):
		self.window_request()
		j = self.process_request()
		self.process_json(j)

	def window_request(self):
		"""Request data from coinAPI in a window around a given timestamp

	    Returns:
	    	request: request containing minute-resolution  data in the range [timestamp - min_before, timestamp + min_after] 
	    """
		payload = {
			'period_id':'1MIN',
			'time_start':self.time_start_iso,
			'time_end':self.time_end_iso,
			'apikey':X_CoinAPI_Key
		}

		r = requests.get(self.url % self.symbol_id, params=payload)
		self.request = r


	def process_request(self):
		if self.request == None:
			return
		
		self.request.raise_for_status()
		try:
			j = self.request.json()
		except Exception as e:
			print(e)
			sys.exit()

		return j

	def __str__(self):
		return self.url % self.symbol_id + '?' + '&'.join([
			'period_id=1MIN',
			'time_start='+self.time_start_iso,
			'time_end='+self.time_end_iso,
			'apikey=ABC123...'
			]) 


class PriceDataRequest(CoinAPIDataRequest):

	def __init__(self, symbol_id, timestamp, msg_hash):
		super().__init__(symbol_id, timestamp, msg_hash)
		self.url = ohlcv_url
		self.outfile = output_price_csv

		if self.is_already_saved():
			self.load()
		else:
			self.run()

	def process_json(self, j):
		if j == None:
			return
		self.dataframe = pd.DataFrame(j)
		self.dataframe['msg_hash'] = self.msg_hash
		self.dataframe['symbol_id'] = self.symbol_id
		self.dataframe['time_tg_message'] = self.timestamp

		self.dataframe['time_period_start'] = self.dataframe['time_period_start'].apply(parse)
		self.dataframe['time_period_end'] = self.dataframe['time_period_end'].apply(parse)
		self.dataframe['time_open'] = self.dataframe['time_open'].apply(parse)
		self.dataframe['time_close'] = self.dataframe['time_close'].apply(parse)

		if not self.is_already_saved():
			self.append_data_to_csv()

	def load(self):
		print('price data already saved for message %s' % self.msg_hash)
		self.dataframe = price_df.loc[price_df.msg_hash == self.msg_hash]



class OrderbookDataRequest(CoinAPIDataRequest):

	def __init__(self, symbol_id, timestamp, msg_hash):
		super().__init__(symbol_id, timestamp, msg_hash)
		self.url = orderbook_url
		self.outfile = output_orderbook_csv

	def process_json(self, j):
		if j == None:
			return

		asks = [{'price':d['price'],'size':d['size'],'time_exchange':data['time_exchange'],
			'time_coinapi':data['time_coinapi'],'type':'ask'} for data in j for d in data['asks']]

		bids = [{'price':d['price'],'size':d['size'],'time_exchange':data['time_exchange'],
			'time_coinapi':data['time_coinapi'],'type':'bid'} for data in j for d in data['bids']]
		
		self.dataframe = pd.DataFrame(asks + bids)
		self.dataframe['message_hash'] = self.hash
		self.dataframe['symbol_id'] = self.symbol_id
		self.dataframe['time_tg_message'] = self.timestamp.isoformat()

		if not self.is_already_saved():
			self.append_data_to_csv()





