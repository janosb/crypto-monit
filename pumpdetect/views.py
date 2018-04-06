import json
import plotly
import boto3
import psycopg2
import plotly.graph_objs as go
import os 
import pandas as pd
import numpy as np

from flask import request
from pumpdetect import app
from flask import Flask, render_template, redirect, url_for
from dateutil.parser import parse

from .sns_config import topic_arn_test, topic_arn_subscription
from .crypto_utils import pg_get_message_data, pg_get_latest_3_messages

sns = boto3.resource('sns')
topic_for_subscription = sns.Topic(topic_arn_subscription)
cl = boto3.client('sns')


@app.route('/')
@app.route('/index')
def index():
    pumps = pg_get_latest_3_messages()   
    return render_template('index.html', pumps=pumps)

@app.route('/add_sms', methods=['POST'])
def add_sms():

    if request.method == 'POST':
       result = request.form
       result_dict = result.to_dict()
    subscription = topic_for_subscription.subscribe(
         Protocol = 'sms',
         Endpoint = result_dict['sms_number']
    )
    return render_template('subscribed_sms.html', result=result_dict)


@app.route('/add_email', methods=['POST'])
def add_email():

   if request.method == 'POST':
      result = request.form
      result_dict = result.to_dict()
      print(result_dict)
   subscription = topic_for_subscription.subscribe(
        Protocol = 'email',
        Endpoint = result_dict['email_address']
   )
   return render_template('subscribed_email.html', result=result_dict)


@app.route('/publish')
def publish_message():
    message = '{\n\t"market":"BTC-FLO,\n\t"timestamp_utc":"2018-01-28 04:12:12", \n\t"market_url":"https://bittrex.com/Market/Index?MarketName=BTC-FLO",\n\t"source":"Telegram",\n\t"channel":"crypto_vip_signal"\n}'
    cl.publish(TopicArn=topic_arn_test, Message=message, Subject='PumpDetect test_message')
    return redirect(url_for('index'))

@app.route('/event', methods=['GET'])
def get_event():
    msg_hash = request.args.get('msg', '')
    msg_data = pg_get_message_data(msg_hash)
    return render_template('event.html', event_data=msg_data) 

@app.route('/learn', methods = ['GET'])
def get_info():
    return render_template('learn.html')

