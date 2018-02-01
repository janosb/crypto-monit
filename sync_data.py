import pandas as pd

from db_connection import conn, cursor, engine, sync_interval	

class TableSyncList(object):

	def __init__(self):
		self.dataframe = None
		self.message_list = []
		self.run()

	def get_list_of_messages(self):
		query = "SELECT msg_hash FROM messages WHERE msg_time BETWEEN (now() at time zone 'utc') - INTERVAL '%s' AND (now() at time zone 'utc');" % sync_interval
		df = pd.read_sql_query(query, conn)	
		print(df.shape)
		self.message_list = df.msg_hash.values

	def process_all_messages(self):
		for msg in self.message_list:
			self.process_one_message()

	def process_one_message(self):
		pass

	def run(self):
		self.get_list_of_messages()
		self.process_all_messages()


class TableSync(object):

	def __init__():
		pass

	def run():
		pass


	def find_missing_data():
		pass



if __name__=='__main__':
	tsl = TableSyncList()
