from dateutil.parser import parse
import pandas as pd 
import requests
import datetime
from tg_api_config import tg_message_file
from coin_api_config import ohlcv_url, X_CoinAPI_Key

def get_tg_df():
	return pd.read_csv(tg_message_file, parse_dates=True)


class PriceDataRequest(object):

	def __init__(self, symbol_id, timestamp):
		super(PriceDataRequest, self).__init__()
		self.symbol_id = symbol_id

		if type(timestamp) == str:
			self.timestamp = parse(timestamp)
		elif type(timestamp) == datetime:
			self.timestamp = timestamp
		else:
			raise TypeError("Unrecognized timestamp type")

		self.minutes_before = -45
		self.minutes_after = 45
		self.time_start_iso = self.offset_timestamp(self.minutes_before)
		self.time_end_iso = self.offset_timestamp(self.minutes_after)

	def offset_timestamp(self, dt):
			return (self.timestamp + datetime.timedelta(minutes = dt)).isoformat()

	def set_minutes_before(self, dt):
		if dt >=0:
			raise ValueError("dt must be negative")
		else:
			self.minutes_before = dt
			self.time_start_iso = self.offset_timestamp(dt)

	def set_minutes_after(self, dt):
		if dt <=0:
			raise ValueError("dt must be positive")
		else:
			self.minutes_after = dt
			self.time_end_iso = self.offset_timestamp(dt)

	def to_str(self):
		return ohlcv_url % self.symbol_id + "?" + "&".join([
			"period_id=1MIN",
			"time_start="+self.time_start_iso,
			"time_end="+self.time_end_iso,
			"X-CoinAPI-Key=ABC123..."
			]) 

	def ohlcv_window_request(self):
		"""Request ohlcv data from coinAPI in a window around a given timestamp

		Args:
			symbol_id (str): coinAPI symbol_id, e.g. "BINANCE_SPOT_RBY_BTC", available from the 
						coinAPI /v1/symbols request
			timestamp (str): window reference point in iso8601 format

		Optional arguments:
	    	min_before: how many minutes before timestamp to request
	    	min_after: how many minutes after timestamp to request

	    Returns:
	    	DataFrame: minute-resolution ohlcv data in the range [timestamp - min_before, timestamp + min_after] 
	    """

		payload = {
			"period_id":"1MIN",
			"time_start":offset_timestamp(-min_before),
			"time_end":offset_timestamp(min_after),
			"X-CoinAPI-Key":X_CoinAPI_Key
		}
		try:
			r = requests.get(ohlcv_url % self.symbol_id, params=payload)
		except Exception as e:
			print(e)
			sys.exit()

if __name__=="__main__":
	tg_df = get_tg_df()

	labeled_df = tg_df.loc[(~tg_df.coin.isnull()) & (tg_df.exchange != "UNKNOWN")].copy()
	test_row = labeled_df.iloc[0]
	symbol_id = "_".join([test_row.exchange.upper(), "SPOT", test_row.coin.upper(), "BTC"])
	print(symbol_id)
	pdr = PriceDataRequest(symbol_id, test_row.time)
	print(pdr.to_str())
	
