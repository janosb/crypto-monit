import pandas as pd
import numpy as np
import datetime

from coin_api_request import PriceWindowRequest
from db_connection import conn, cursor, engine, sync_interval	

class TableSyncList(object):

	def __init__(self):
		self.dataframe = None
		self.message_list = []
		self.run()

	def get_list_of_messages(self):
		query = "SELECT msg_hash, msg_time FROM messages WHERE msg_time BETWEEN (now() at time zone 'utc') - INTERVAL '%s' AND (now() at time zone 'utc');" % sync_interval
		df = pd.read_sql_query(query, conn)	
		print(df.shape)
		for index, h in df.iterrows():
			self.message_list.append({'msg_hash':h.msg_hash, 'msg_time':h.msg_time})

	def process_all_messages(self):
		for msg in self.message_list:
			self.process_one_message(msg)

	def process_one_message(self, msg_dict):
		ts = TableSync(msg_dict)

	def run(self):
		self.get_list_of_messages()
		self.process_all_messages()


class TableSync(object):

	def __init__(self, msg_dict):
		self.msg_hash = msg_dict['msg_hash']
		self.msg_time = msg_dict['msg_time']
		self.symbol_id = None
		self.run()

	def run(self):
		self.find_missing_data()


	def find_missing_data(self):
		query = "SELECT time_period_end, symbol_id FROM price_data WHERE msg_hash = \'%s\' ORDER BY time_period_end DESC LIMIT 1" % self.msg_hash;
		df = pd.read_sql_query(query, conn)	
		if df.shape[0] == 0:
			# we assume that all messages have been registered at least once
			return
		else:
			t_latest = df.time_period_end.values[0]
			self.symbol_id = df.symbol_id.values[0]
			t_45 = np.datetime64(self.msg_time + datetime.timedelta(minutes = 45))
			if t_45 - t_latest > np.timedelta64(5,'m'):
				self.download_missing_data(t_latest, t_45)

	def download_missing_data(self, t_start, t_end):
			print('requesting %s from %s to %s with hash %s' % (self.symbol_id, t_start, t_end, self.msg_hash))
			pwr = PriceWindowRequest(self.symbol_id, self.msg_time, self.msg_hash, t_start, t_end)

			

if __name__=='__main__':
	tsl = TableSyncList()




