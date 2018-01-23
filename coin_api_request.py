import pandas as pd 
import requests
import datetime
import hashlib
import os, sys
import json

from dateutil.parser import parse
from tg_api_config import tg_cleaned_file
from coin_api_config import *

def get_tg_df():
	return pd.read_csv(tg_cleaned_file, parse_dates=['time'])

def get_fake_request_data():
	return open(fake_request_data, 'r').read()


class CoinAPIDataRequest(object):

	def __init__(self, symbol_id, timestamp, label):
		self.symbol_id = symbol_id
		self.label = label

		if type(timestamp) == str:
			self.timestamp = parse(timestamp)
		elif type(timestamp) == datetime:
			self.timestamp = timestamp
		else:
			raise TypeError('Unrecognized timestamp type: %s', timestamp)

		self.minutes_before = -45
		self.minutes_after = 45
		self.time_start_iso = self.offset_timestamp(self.minutes_before)
		self.time_end_iso = self.offset_timestamp(self.minutes_after)
		self.calculate_hash()
		self.request = None
		self.dataframe = None

	def calculate_hash(self):
		hash_str = ':'.join([self.timestamp.isoformat(), self.symbol_id])
		self.hash = hashlib.sha256(hash_str.encode()).hexdigest()

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

		if self.hash in open(self.outfile).read():
			print('found hash in csv: %s' % self.hash)
			return True

		return False

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

	def to_str(self):
		return self.url % self.symbol_id + '?' + '&'.join([
			'period_id=1MIN',
			'time_start='+self.time_start_iso,
			'time_end='+self.time_end_iso,
			'apikey=ABC123...'
			]) 


class PriceDataRequest(CoinAPIDataRequest):

	def __init__(self, symbol_id, timestamp, label):
		super().__init__(symbol_id, timestamp, label)
		self.url = ohlcv_url
		self.outfile = output_price_csv

	def process_json(self, j):
		if j == None:
			return
		self.dataframe = pd.DataFrame(j)
		self.dataframe['message_hash'] = self.hash
		self.dataframe['symbol_id'] = self.symbol_id
		self.dataframe['time_tg_message'] = self.timestamp.isoformat()
		self.dataframe['label'] = self.label
		
		if not self.is_already_saved():
			self.append_data_to_csv()


class OrderbookDataRequest(CoinAPIDataRequest):

	def __init__(self, symbol_id, timestamp, label):
		super().__init__(symbol_id, timestamp, label)
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
		self.dataframe['label'] = self.label

		if not self.is_already_saved():
			self.append_data_to_csv()





