import pandas as pd 
import requests
import datetime
import hashlib
import os, sys
import json

from dateutil.parser import parse
from tg_api_config import tg_message_file
from coin_api_config import *

def get_tg_df():
	return pd.read_csv(tg_message_file, parse_dates=True)

def get_fake_request_data():
	return open(fake_request_data, 'r').read()


class CoinAPIDataRequest(object):

	def __init__(self, symbol_id, timestamp):
		self.symbol_id = symbol_id

		if type(timestamp) == str:
			self.timestamp = parse(timestamp)
		elif type(timestamp) == datetime:
			self.timestamp = timestamp
		else:
			raise TypeError('Unrecognized timestamp type')

		self.minutes_before = -45
		self.minutes_after = 45
		self.time_start_iso = self.offset_timestamp(self.minutes_before)
		self.time_end_iso = self.offset_timestamp(self.minutes_after)
		self.request = None
		self.dataframe = None

	def calculate_hash(self):
		hash_str = ':'.join([self.timestamp.isoformat(), self.symbol_id, self.type])
		return hashlib.sha256(hash_str.encode()).hexdigest()

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
		try:
			r = requests.get(self.url % self.symbol_id, params=payload)
			self.request = r
		except Exception as e:
			print(e)
			sys.exit()

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

	def __init__(self, symbol_id, timestamp):
		super().__init__(symbol_id, timestamp)
		self.type = 'ohlcv:historical'
		self.hash = self.calculate_hash()
		self.url = ohlcv_url
		self.outfile = output_price_csv

	def process_json(self, j):
		if j == None:
			return
		self.dataframe = pd.DataFrame(j)
		self.dataframe['message_hash'] = self.hash
		self.dataframe['symbol_id'] = self.symbol_id
		
		if not self.is_already_saved():
			self.append_data_to_csv()


class OrderbookDataRequest(CoinAPIDataRequest):

	def __init__(self, symbol_id, timestamp):
		super().__init__(symbol_id, timestamp)
		self.type = 'orderbook:historical'
		self.url = orderbook_url
		self.hash = self.calculate_hash()
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
		
		if not self.is_already_saved():
			self.append_data_to_csv()


if __name__=='__main__':
	tg_df = get_tg_df()

	labeled_df = tg_df.loc[(~tg_df.coin.isnull()) & (tg_df.exchange != 'UNKNOWN')].copy()

	iterable = zip(labeled_df['coin'],labeled_df['exchange'],labeled_df['time'])
	for t in list(iterable):
		symbol_id = '_'.join([t[1].upper(), 'SPOT', t[0], 'BTC'])
		print(symbol_id)
		pdr = OrderbookDataRequest(symbol_id, t[2])
		if not pdr.is_already_saved():
			pdr.window_request()
			pdr.process_json(pdr.process_request())



