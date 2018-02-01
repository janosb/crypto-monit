import pandas as pd
import numpy as np
import datetime
import math
import itertools

from dateutil.parser import parse
from coin_api_config import * 




def get_price_data():
	# price_df["dayofweek"] = price_df.time_tg_message.dt.dayofweek
	return pd.read_csv(output_price_csv, parse_dates=['time_tg_message','time_close','time_open',
										'time_period_start','time_period_end'])

def process_price_data(df):
	price_groups = df.groupby('message_hash', sort=False)
	feature_df = pd.DataFrame()

	start_offsets = range(-15, 20, 5)
	cols = ['volume_traded', 'price_close']
	stats = ['mean','sum']
	#stats = ['mean','sum','range','std']
	for s, col, stat in list(itertools.product(start_offsets, cols, stats)):
		interval = [s, s+5]
		col_name = "%s_%d_%d_%s" % (col, abs(s), abs(s+5), stat)
		feature_df[col_name] = price_groups.apply(get_window_stats, interval, col, stat, 'time_period_start', 'time_period_end')

	for int_pair, col, stat in list(itertools.product(interval_pairs, cols, stats)):
		start1, end1 = int_pair[0]
		start2, end2 = int_pair[1]	
		col_name = "%s_%s_%d_%d_%d_%d_%s" % (col, 'ratio', abs(start1), abs(end1), abs(start2), abs(end2), stat)
		print(col_name)
		feature_df[col_name] = price_groups.apply(get_window_ratios, int_pair[0], int_pair[1], 
								col, stat, 'time_period_start', 'time_period_end')

	feature_df['label'] = price_groups.first().label
	return feature_df

def get_orderbook_data():
	return pd.read_csv(output_orderbook_csv, parse_dates=['time_tg_message','time_coinapi','time_exchange'])

def process_orderbook_data(df):
	orderbook_groups = df.groupby(['message_hash','type'], sort=False)
	feature_df = pd.DataFrame()
	
	start_offsets = range(-15, 20, 5)
	cols = ['price', 'size']
	stats = ['mean','sum','range','std']

	for s, col, stat in list(itertools.product(start_offsets, cols, stats)):
		interval = [s, s+5]
		col_name = col + "_%d_%d_%s" % (abs(s), abs(s+5), stat)
		feature_df[col_name] = orderbook_groups.apply(get_window_stats, interval, col, stat, 'time_coinapi', 'time_coinapi')


	feature_df['label'] = orderbook_groups.first().label
	feature_df['type'] = orderbook_groups.first().type
	print(feature_df.iloc[0])
	return feature_df

def merge_features_and_save(price_df, orderbook_df):
	out_df = price_df.join(orderbook_df, on='message_hash')
	out_df.to_csv(output_features_csv)





def get_feature_zip(request_type):
	if request_type == 'ratio':
		interval_pairs = [
						#([-10,-5],[-5,0]),
						#([-20,-5],[-5,0]),
						#([-45,-5],[-5,0]),
						#([-20,-10],[-10,0]),
						([-45,-10],[-10,0])
					]

		cols = ['trades_count','volume_traded', 'price_close']
		stats = ['pct_change_mean', 'mean_over_std']
		return list(itertools.product(interval_pairs, cols, stats))

	if request_type == 'stat':
		intervals = [
					[-15, -10],
					[-10, -5],
					[-5, 0]
					]
		cols = ['volume_traded', 'trades_count']
		stats = ['mean','sum']

		return list(itertools.product(intervals, cols, stats))





class PriceFeatures(object):

	def __init__(self, price_df, message_hash):
		self.raw_price_df = price_df
		self.message_hash = message_hash

		ts = price_df.time_tg_message.values[0]
		if isinstance(ts, str):
			self.time_tg_message = np.datetime64(parse(ts))
		elif isinstance(ts, datetime.datetime):
			self.time_tg_message = np.datetime64(ts)
		elif isinstance(ts, np.datetime64):
			self.time_tg_message = ts
		else:
			raise TypeError('Unsupported timestamp type: %s', ts)

		self.features = {}
		self.label = None

		self.calculate_features()
		self.calculate_label()

	def __repr__(self):
		return "hash: %s\ntg_message_time: %s" % (self.message_hash, self.time_tg_message)

	def printout(self):
		print("Hash:", self.message_hash)
		print("Time:", self.time_tg_message)
		print("Features:", self.features)
		print("Label:", self.label)

	def calculate_features(self):
		# stat features
		for tup in get_feature_zip('stat'):
			start = tup[0][0]
			end = tup[0][1] 
			col = tup[1]
			stat = tup[2]
			self.add_stat_feature(start, end, col, stat)
		# ratio features
		for tup in get_feature_zip('ratio'):
			start1 = tup[0][0][0]
			end1 = tup[0][0][1]
			start2 = tup[0][1][0]
			end2 = tup[0][1][1]
			col = tup[1]
			stat = tup[2]
			self.add_ratio_feature(start1, end1, start2, end2, col, stat)

	def calculate_label(self):
		time_start_1 = -10
		time_end_1 = 0
		time_start_2 = 0
		time_end_2 = 20

		self.label = self.get_window_ratios(time_start_1, time_end_1, time_start_2, time_end_2, 'price_close', 'pct_change_max')


	def add_stat_feature(self, time_start, time_end, col, stat):
		feat_name = "%s_%s_%d_%d_%s" % (col, 'stat', abs(time_start), abs(time_end), stat)
		self.features[feat_name] = self.get_window_stats(time_start, time_end, col, stat)

	def add_ratio_feature(self, time_start_1, time_end_1, time_start_2, time_end_2, col, stat):
		feat_name = "%s_%s_%d_%d_%d_%d_%s" % (col, 'ratio', abs(time_start_1), abs(time_end_1), 
														 abs(time_start_2), abs(time_end_2), stat)
		self.features[feat_name] = self.get_window_ratios(time_start_1, time_end_1, time_start_2, time_end_2, col, stat)


	def get_window(self, time_start, time_end, time_start_col, time_end_col):

		#t_window_start = self.time_tg_message + datetime.timedelta(minutes=time_start)
		t_window_start = self.time_tg_message + np.timedelta64(time_start,'m')
		t_window_end = self.time_tg_message + np.timedelta64(time_end,'m')
		#t_window_end = self.time_tg_message + datetime.timedelta(minutes=time_end)
		return self.raw_price_df.loc[(self.raw_price_df[time_start_col] > t_window_start) &
									 (self.raw_price_df[time_end_col] < t_window_end)]

	def get_window_stats(self, time_start, time_end, col, stat, time_start_col = 'time_period_start', 
														 time_end_col = 'time_period_end'):
		window = self.get_window(time_start, time_end, time_start_col, time_end_col)
		if stat == 'sum':
			return window[col].sum()	
		if stat == 'avg' or stat == 'mean':
			return window[col].mean()
		if stat == 'range':
			return window[col].max() - window[col].min()
		if stat == 'std':
			return window[col].std()
		else:
			raise ValueError("Stat type not supported: %s" % stat)

	def get_window_ratios(self, time_start_1, time_end_1, time_start_2, time_end_2, col, stat, 
							time_start_col = 'time_period_start', time_end_col = 'time_period_end'):
		win1 = self.get_window(time_start_1, time_end_1, time_start_col, time_end_col)
		win2 = self.get_window(time_start_2, time_end_2, time_start_col, time_end_col)
		try:
			if stat == 'sum':
				return win2[col].sum()/win1[col].sum()	
			if stat == 'max':
				return win2[col].max()/win1[col].max()	
			if stat == 'avg' or stat == 'mean':
				return win2[col].mean()/win1[col].mean()
			if stat == 'std':
				return win2[col].std()/win1[col].std()
			if stat == 'pct_change_mean':
				return (win2[col].mean()-win1[col].mean())/win1[col].mean()
			if stat == 'pct_change_max':
				return (win2[col].max()-win1[col].min())/win1[col].min()
			if stat == 'mean_over_std':
				return (win2[col].mean()-win1[col].mean())/win1[col].std()
			else:
				raise ValueError("Stat type not supported: %s" % stat)	
		except ZeroDivisionError:
			return math.nan
		


if __name__=='__main__':
	price_df = get_price_data()
	price_groups = price_df.groupby('message_hash', sort=False).groups
	pfs = []
	for msg_hash in price_groups.keys():
		#print(price_df.iloc[price_groups[msg_hash]])
		pf = PriceFeatures(price_df.iloc[price_groups[msg_hash]], msg_hash)






