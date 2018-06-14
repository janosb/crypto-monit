# Cryptomarket monitoring



This is a project aiming to monitor altcoin markets and detect "pump-and-dump" events in real-time using a combination of Telegram and OHLCV data.

## Requirements

API authentication keys from [Telegram](https://core.telegram.org/#telegram-api) and [coinAPI](https://docs.coinapi.io/#introduction). 

Required packages can be found in requirements.txt. It is highly recommended to use a virtualenv with Python 3.6 or later. 

```
pip install -r requirements.txt

```

There are a few dependencies that NLTK asks you to download, so go ahead and run the following in python:

```
import nltk
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')

```

## Configuration 

Config templates are available in tg_api_config.py.template and coin_api_config.py.template. Enter your information and rename to tg_api_config.py and coin_api_config.py.





