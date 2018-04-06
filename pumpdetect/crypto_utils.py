import pandas as pd
import psycopg2
import datetime
import os
from flask import json

from .db_config import *

conn = psycopg2.connect(database = pg_db, user = pg_user, password = pg_password, port = pg_port, host = pg_host)

def pg_get_message_data(msg_hash):
    query = 'SELECT * FROM price_data WHERE msg_hash = \'%s\' ORDER BY time_period_end' % msg_hash
    msg_data = pd.read_sql_query(query, conn)
    conn.rollback()
    msg_dict = {}
    md = {}
    if not msg_data.shape[0] == 0:
        ts_str = str(msg_data.time_tg_message.values[0])
        msg_dict['msg_hash'] = msg_hash
        md['price_close'] = list(msg_data.price_close.values*1.e8)
        md['price_open'] = list(msg_data.price_open.values*1.e8)
        md['price_high'] = list(msg_data.price_high.values*1.e8)
        md['price_low'] = list(msg_data.price_low.values*1.e8)
        #msg_dict['time_period_end'] = list(msg_data.time_period_end.values)
        md['time_period_end'] = list(msg_data.time_period_end.apply(str).values)
        msg_dict['mkt'] = "-".join(msg_data.symbol_id.values[0].split('_')[2:])
        msg_dict['timestamp'] = clean_tg_timestamp(ts_str)
        msg_dict['data'] = [{'x':md['time_period_end'], 'close':md['price_close'], 'open':md['price_open'], 'high':md['price_high'],'low':md['price_low'],'type':'candlestick', 'xaxis': 'x', 'yaxis': 'y', 'increasing':{'line':{'color':'#5DBCD2'}}}]
        msg_dict['layout'] = {"yaxis":{"title":"Price (Satoshi)"},'dragmode': 'zoom','showlegend': False,'xaxis': {'rangeslider':{'visible': False}},'shapes': [{'type': 'line','x0': ts_str,'y0': min(md['price_low']),'x1': ts_str,'y1': max(md['price_high']),'line': {'color': '#000000','width': 3}}]}
    return msg_dict


def pg_get_latest_3_messages(min_time="now()"):
    query = 'SELECT msg_hash, msg_time FROM messages WHERE msg_time < \'%s\' ORDER BY msg_time DESC LIMIT 30;' % min_time
    print(query)
    msg_df = pd.read_sql_query(query, conn)
    conn.rollback()
    data = []
    for index, row in msg_df.iterrows():
        msg_hash = row['msg_hash']
        min_time = row['msg_time']
        msg_data = pg_get_message_data(msg_hash)
        if msg_data:
                data.append(msg_data)
        if len(data) == 3:
            return data
    return pg_get_latest_3_messages(min_time) 
    

def clean_tg_timestamp(ts):
    return "Message at %s UTC" % str(ts).replace("T", " ").replace(".000000000","")

def clean_timestamp(ts):
    raise ValueError("type %s" % type(ts))
    ts_obj = ts.astype(object)
    return "%d:%d:%d UTC" % (ts_obj.hour, ts_obj.minute, ts_obj.second)


if __name__=='__main__':
    print(pg_get_latest_3_messages())


