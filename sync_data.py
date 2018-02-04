import pandas as pd
import numpy as np
import datetime

from coin_api_request import PriceWindowRequest
from db_connection import conn, cursor, engine, sync_interval	
from logs import cron_log

class TableSyncList(object):
	""" Loads a list of messages and creates TableSync objects for processing
	
	The goal of these two classes is to fill in missing price/volume data that 
	haven't been downloaded. This is necessary for real-time monitoring of TG messages.

	Attributes:
	    message_list (list of dict): list of message hashes/timestamps to process 

	"""

	def __init__(self):
		self.message_list = []
		self._run()

	def get_list_of_messages(self):
		query = """SELECT msg_hash, msg_time FROM messages WHERE msg_time 
			BETWEEN (now() at time zone 'utc') - INTERVAL '%s' 
			AND (now() at time zone 'utc');""" % sync_interval
		df = pd.read_sql_query(query, conn)	
		for index, h in df.iterrows():
			self.message_list.append({'msg_hash':h.msg_hash, 'msg_time':h.msg_time})

	def process_all_messages(self):
		try:
			logfile = open(cron_log, 'a')
		except FileNotFoundError:
			print('error opening file %s\n' % cron_log)
			logfile = None

		for msg in self.message_list:
			self.process_one_message(msg, logfile)

	def process_one_message(self, msg_dict, logfile):
		ts = TableSync(msg_dict, logfile)

	def _run(self):
		self.get_list_of_messages()
		self.process_all_messages()


class TableSync(object):
	""" Stores message identifiers and triggers the API request to fill in missing data

	Attributes:
	    msg_hash (str): sha256 hash of the message
	    msg_time (datetime.datetime): time at which message was published in TG
	    symbol_id (str): the CoinAPI market identifier associated with this coin/exchange combination
	    log (file): open logfile for recording what was downloaded

	"""

	def __init__(self, msg_dict, log=None):
		self.msg_hash = msg_dict['msg_hash']
		self.msg_time = msg_dict['msg_time']
		self.symbol_id = None
		self.log = log
		self._run()

	def _run(self):
		self.find_missing_data()

	def find_missing_data(self):
		query = """SELECT time_period_end, symbol_id FROM price_data 
			WHERE msg_hash = \'%s\' ORDER BY time_period_end DESC 
			LIMIT 1""" % self.msg_hash;

		df = pd.read_sql_query(query, conn)	

		if df.shape[0] == 0:
			# we assume that all messages have been registered at least once
			return
		else:
			t_latest = df.time_period_end.values[0]
			self.symbol_id = df.symbol_id.values[0]
			t_45 = np.datetime64(self.msg_time 
				+ datetime.timedelta(minutes = 45))
			if t_45 - t_latest > np.timedelta64(5,'m'):
				self.download_missing_data(t_latest, t_45)

	def download_missing_data(self, t_start, t_end):
		print("downloading data")
		if self.log:
			self.log.write('requesting %s from %s to %s with hash %s\n' 
				% (self.symbol_id, t_start, t_end, self.msg_hash))
		pwr = PriceWindowRequest(self.symbol_id, self.msg_time, self.msg_hash, t_start, t_end)

			

if __name__=='__main__':
	tsl = TableSyncList()




