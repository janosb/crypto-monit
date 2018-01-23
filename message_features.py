import json
import html.parser
import re
import pandas
import sklearn as sk

from sklearn.model_selection import train_test_split
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from tg_api_config import tg_cleaned_file, coin_map_file
from process_messages import get_clean_data_df

class MessageFeatures(object):

	def __init__(self, message):
		self.raw_message = message.replace('\r',' ')
		self.clean_message = None
		self.tokens = []
		self.features = {}
		self.coin_map = self.load_coin_map()
		self.first_coin_mentioned = None
		self.run()

	@classmethod
	def create_instance(cls, message):
		if type(message) != str:
			return None
		return cls(message)

	def run(self):
		self.clean()
		self.tokenize()
		self.calculate_features()

	def load_coin_map(self):
		return json.load(open(coin_map_file))	

	def clean(self):
		html_parser = html.parser.HTMLParser()
		msg = html_parser.unescape(self.raw_message)
		msg = msg.replace(':', ' ').replace('/','  ').replace('#', ' ').replace('-', ' ')
		self.clean_message = msg

	def tokenize(self):
		tokens = word_tokenize(self.clean_message)
		stop_words = set(stopwords.words('english'))
		filtered_tokens = [w for w in tokens if not w in stop_words]

		pos_tokens = pos_tag(filtered_tokens)
		for tok in pos_tokens:
			self.tokens.append((tok[0].upper(), tok[1]))

	def calculate_features(self):
		self.first_coin_mentioned, self.features['n_coins_mentioned'] = self.search_for_coins()
		self.features['contains_coin_mention'] = 1 if self.features['n_coins_mentioned'] > 0 else 0
		self.features['has_a_number'] = self.has_a_number()
		self.features['contains_word_target'] = self.contains_word('target')
		self.features['contains_word_buy'] = self.contains_word('buy')
		self.features['contains_word_sell'] = self.contains_word('sell')
		self.features['contains_word_btc'] = self.contains_word('btc')
		self.features['contains_exchange_name'] = self.contains_exchange()
	
	def to_dict(self):
		return self.features

	def to_list(self):
		return list(self.features.values())

	# FEATURE EXTRACTION FUNCTIONS
	def search_for_coins(self):
		first_coin = None
		n_coins = 0

		for tok in self.tokens:
			try: 
				found_coin = self.coin_map[tok[0]]
				if not first_coin: 
					first_coin = found_coin
				n_coins +=1
			except KeyError:
				pass
		return first_coin, n_coins

	def has_a_number(self):
		for tok in self.tokens:
			if tok[1] == 'CD' : return 1
		return 0

	def contains_word(self, word):
		w = word.upper()
		for tok in self.tokens:
			if tok[0] == w : return 1
		return 0

	def contains_exchange(self):
		exchanges = ["BITTREX", "POLONIEX", "BINANCE"]
		for tok in self.tokens:
			if tok[0] in exchanges : return 1
		return 0		



class MessageClassifier(object):

	def __init__(self, features, labels):
		self.features = features.values
		self.labels = labels
		self.X_train = None
		self.X_test = None
		self.y_train = None
		self.y_test = None
		self.model = None
		self.run()

	@classmethod
	def initialize_classifier(cls, features, labels):
		if type(features) != pandas.DataFrame:
			raise TypeError("Features should be in a pandas DataFrame")
		if type(labels) != list:
			raise TypeError("Labels should be in a list")
		if features.shape[0] != len(labels):
			raise ValueError("Features and Labels should be same size")
		return cls(features, labels)

	@classmethod
	def initialize_from_file(cls):
		df, labels = get_all_message_features()
		return MessageClassifier.initialize_classifier(df, labels)

	def run(self):
		self.split_data()
		self.set_model_to_decision_tree()
		self.validate_model()		

	def split_data(self):
		self.X_train, self.X_test, self.y_train, self.y_test = \
				train_test_split(self.features, self.labels, test_size=0.8)

	def validate_model(self):
		print("Accuracy on the test set: ", self.model.score(self.X_test, self.y_test))

	# DIFFERENT CLASSIFIERS TO TRY
	def set_model_to_decision_tree(self):
		from sklearn.tree import DecisionTreeClassifier
		clf = DecisionTreeClassifier()
		self.model = clf.fit(self.X_train, self.y_train)

	def test_messages(self):
		while 1:
			msg = input('Enter message:')
			mf = MessageFeatures.create_instance(msg)
			feats = pandas.DataFrame([mf.to_dict()])
			prediction = self.model.predict(feats)
			if prediction == 0: 
				print("Not likely to be a Pump...")
			else: 
				print("Probably a Pump!")

	def predict_message(self, msg_features):
		feats = pandas.DataFrame([msg_features.to_dict()])
		return self.model.predict(feats)

def get_all_message_features():
	df = get_clean_data_df()
	mfs = []
	labels = []
	for i in range(df.shape[0]):
		mf = MessageFeatures.create_instance(df.iloc[i]['message'])
		if mf:
			mfs.append(mf.to_dict())
			labels.append(df.iloc[i]['label'])

	return pandas.DataFrame(mfs), labels






if __name__=='__main__':
	df, labels = get_all_message_features()
	clf = MessageClassifier.initialize_classifier(df, labels)

	#clf.test_messages()




