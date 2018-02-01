import pandas as pd
import numpy as np
import json
import html.parser

from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from tg_api_config import tg_cleaned_file, coin_map_file
from process_messages import get_clean_data_df


class MessageFeatures(object):
	"""
		Process the raw text from a telegram message
	"""
	def __init__(self, message):
		self.raw_message = message.replace('\r',' ')
		self.clean_message = None
		self.tokens = []
		self.features = {}
		self.coin_map = self.load_coin_map()
		self.first_coin_mentioned = None
		self.first_exchange_mentioned = None
		self.run()

	@classmethod
	def create_instance(cls, message):
		if not isinstance(message, str):
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
		msg = msg.replace(':', ' ').replace('/','  ').replace('#', ' ').replace('-', ' ').replace('!', ' ')
		self.clean_message = msg

	def tokenize(self):
		tokens = word_tokenize(self.clean_message)
		self.features['n_words_total'] = len(tokens)

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
		self.features['contains_word_btc'] = self.contains_word('btc') or self.contains_word('bitcoin')
		self.features['contains_exchange_name'] = self.contains_exchange()
		self.features['n_words_total'] = len(self.tokens)

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
			if tok[0] in exchanges : 
				self.first_exchange_mentioned = tok[0]
				return 1
		return 0		




def get_all_message_features():
	df = get_clean_data_df()
	mfs = []
	labels = []
	for i in range(df.shape[0]):
		msg = df.iloc[i]['message']
		mf = MessageFeatures.create_instance(msg)
		if mf:
			mfs.append(mf.to_dict())
			labels.append(df.iloc[i]['label'])

	return pd.DataFrame(mfs), labels


def get_price_features():
	features_df = pd.read_csv(output_features_csv)
	message_label = 'label'
	price_label = 'price_close_ratio_5_0_0_20_pct_change'
	feats  = [
				'price_close_ratio_45_5_5_0_mean_over_std', 
				'price_close_ratio_45_10_10_0_mean_over_std', 
				'price_close_ratio_20_10_10_0_mean_over_std',
				'volume_traded_ratio_45_5_5_0_mean_over_std',
				'volume_traded_ratio_20_5_5_0_pct_change',
				'volume_traded_ratio_20_5_5_0_mean',
				'volume_traded_ratio_45_10_10_0_sum',
				'volume_traded_ratio_45_10_10_0_pct_change',
			]
	both = feats.copy()
	both.append(price_label	)
	both_df = features_df.loc[features_df[message_label] == 1, both]
	df2 = both_df.replace([np.inf, -np.inf], np.nan)
	df2.dropna(inplace=True)

	df2['price_label'] = df2['price_close_ratio_5_0_0_20_pct_change'].apply(lambda x: 1 if x > 0.05 else 0)
	df2.drop('price_close_ratio_5_0_0_20_pct_change', axis=1, inplace=True)

	return df2.loc[:,feats], list(df2.loc[:,'price_label'].values)


if __name__=='__main__':
	#Messages
	df, labels = get_all_message_features()





