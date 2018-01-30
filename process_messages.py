import pandas as pd
from tg_api_config import *
from coin_api_request import *
from dateutil.parser import parse
from math import nan
import pytz
import sys

def remove_repeats(msg_df):
	repeat_groups = msg_df.loc[msg_df.repeat == 1].groupby('message')
	df = pd.DataFrame(repeat_groups.views.apply(sum))
	df = repeat_groups.first()
	df.views = repeat_groups.views.apply(sum)
	df.channel = 'multiple'

	other_msgs = msg_df.loc[msg_df.repeat == 0]
	df_concat = pd.concat([df.reset_index(), other_msgs.reset_index()])
	df_concat.loc[(df_concat.exchange == 'UNKNOWN'), 'exchange'] = 'BITTREX'

	return df_concat

def pnd_events(df):
	pnd_events = df[df.label == 1]
	hist1 = pnd_events.exchange.hist()

	coin_counts = pnd_events.groupby(pnd_events.coin).apply(lambda x:x.views.size)
	hist2 = coin_counts.hist(bins = 8)

def get_symbol_id(coin, exchange):
	# handle NaNs
	if type(coin) == float:
		return None

	coin_s = coin.split('-')
	if len(coin_s) == 2:
		coin_from = coin_s[0]
		coin_to = coin_s[1]
	elif len(coin_s) == 1:
		coin_from = coin
		coin_to = 'BTC'
	else:
		raise ValueError("Coin misshaped: %s" % coin)

	if coin_from == coin_to:
		return None

	return '_'.join([exchange, 'SPOT', coin_from, coin_to])

def add_symbol_id(df):
	df['symbol_id'] = df[['coin', 'exchange']].apply(lambda x: get_symbol_id(x[0], x[1]), axis=1)

def get_clean_data_df():
	original_df = pd.read_csv(tg_cleaned_file, encoding='latin-1', parse_dates=['time'], index_col='index')
	df = remove_repeats(original_df)
	add_symbol_id(df)
	return df	

if __name__=='__main__':
	df = get_clean_data_df()

