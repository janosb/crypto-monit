import pandas as pd
import matplotlib.pyplot as plt
import sklearn as sk
import numpy as np

from sklearn.model_selection import train_test_split, KFold
from sklearn.metrics import roc_curve, confusion_matrix, average_precision_score, accuracy_score
from coin_api_config import output_features_csv
from message_features import get_all_message_features

class GeneralClassifier(object):

	def __init__(self, features_df, labels):
		self.features = features_df.values
		self.feature_names = features_df.columns
		self.labels = np.array(labels)
		self.X_train = None
		self.X_test = None
		self.y_train = None
		self.y_test = None
		self.model = None
		self.run()

	@classmethod
	def initialize_classifier(cls, features, labels):
		if type(features) != pd.DataFrame:
			raise TypeError("Features should be in a pandas DataFrame")
		if type(labels) != list:
			raise TypeError("Labels should be in a list")
		if features.shape[0] != len(labels):
			raise ValueError("Features and Labels should be same size")
		return cls(features, labels)

	@classmethod
	def initialize_from_file(cls):
		# this is only for Message classification!

		df, labels = get_all_message_features()
		return GeneralClassifier.initialize_classifier(df, labels)

	def run(self):
		self.split_data()
		self.set_model_to_random_forest()
		self.validate_model()		

	def split_data(self):
		self.X_train, self.X_test, self.y_train, self.y_test = \
				train_test_split(self.features, self.labels, test_size=0.8)

	# DIFFERENT CLASSIFIERS TO TRY
	def set_model_to_decision_tree(self):
		from sklearn.tree import DecisionTreeClassifier
		clf = DecisionTreeClassifier()
		self.model = clf.fit(self.X_train, self.y_train)

	def set_model_to_random_forest(self):
		from sklearn.ensemble import RandomForestClassifier
		clf = RandomForestClassifier(n_estimators = 50)
		self.model = clf.fit(self.X_train, self.y_train)

	def set_model_to_random_forest_reg(self):
		from sklearn.ensemble import RandomForestRegressor
		regr = RandomForestRegressor(max_depth=2, random_state=0)
		self.model = regr.fit(self.X_train, self.y_train)

	def predict_message(self, msg_features):
		feats = pd.DataFrame([msg_features.to_dict()])
		return self.model.predict(feats)

	def validate_model(self, show=False):
		probs = self.model.predict_proba(self.X_test)
		predictions = self.model.predict(self.X_test)
		print("Train Accuracy :: ", accuracy_score(self.y_train, self.model.predict(self.X_train)))
		print("Test Accuracy  :: ", accuracy_score(self.y_test, predictions))
		print(" Confusion matrix ", confusion_matrix(self.y_test, predictions))
		pred_proba = self.model.predict_proba(self.X_test)
		pred_proba = pd.DataFrame(pred_proba)
		fpr, tpr, thresholds = roc_curve(self.y_test, pred_proba.iloc[:,1], pos_label=1)
		if show:
			self.plot_roc(fpr, tpr)
		return fpr, tpr, thresholds, confusion_matrix

	def k_fold_cv(self):
		k_fold = KFold(n_splits=3)
		print([self.model.fit(self.features[train], self.labels[train]).score(self.features[test], self.labels[test])
					for train, test in k_fold.split(self.features)])

	def plot_roc(self, fpr, tpr):
		plt.title('ROC Curve')
		plt.xlabel('False Positive Rate')
		plt.ylabel('True Positive Rate')
		plt.plot(fpr, tpr)
		random_x = np.linspace(0,1,100)
		plt.plot(random_x, random_x, '--')
		auc = sk.metrics.auc(fpr, tpr)
		plt.text(.4, .6, "AUC: %3.1f%%" % (auc*100))
		plt.show()

	def feature_importance(self, n_show, show=False):
		importances = self.model.feature_importances_
		ix = np.argsort(importances)[::-1]
		print(importances[ix])
		importances_sorted = importances[ix[0:n_show]]
		feature_names_sorted = self.feature_names[ix[0:n_show]]
		if show:
			fig, ax = plt.subplots()
			plt.title('Message Classification: Importance of Features')
			plt.xlabel('Importance')
			plt.margins(0.2)
			plt.subplots_adjust(left=0.45)
			y_pos = np.arange(n_show)
			ax.barh(y_pos, importances_sorted, align='center')
			ax.set_yticks(y_pos)
			ax.set_yticklabels(feature_names_sorted)
			ax.invert_yaxis()
			plt.show()

def cleanup(string):
	return string.replace("_"," ").title()


if __name__=='__main__':
	features_df = pd.read_csv('mixed_features.csv')
	labels_df = pd.read_csv('mixed_labels.csv')

	both_df = pd.concat([features_df, labels_df], axis=1).dropna()


	features_df = both_df.drop(columns = ['label', 'msg_hash'])
	labels_series = both_df.loc[:,'label'].apply(lambda x: 1 if x > 0.10 else 0)
	clf = GeneralClassifier.initialize_classifier(features_df, list(labels_series))
	clf.validate_model(show=True)
	clf.feature_importance(5, show=True)
	clf.k_fold_cv()







