# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import pandahouse
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Функция для CH
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', 
              user='***', password='***'):
    connection = {
    'host': host,
    'password': password,
    'user': user,
    'database': 'simulator_20230620'
}
    df = pandahouse.read_clickhouse(query, connection=connection)
    return df


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a-hadchukaev-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 10),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, 
     schedule_interval=schedule_interval, 
     catchup=False)

def dag_khadchukaev():

    @task
    def extract_df_feed():
        query = """select toDate(time) AS event_date,
                    user_id, 
                    countIf(action='like') as likes,  
                    countIf(action='view') as views,
                    os, if(gender=0, 'male', 'female') as gender, age
                   from simulator_20230620.feed_actions
                   where toDate(time) = today()-1
                   group by event_date, user_id, os, gender, age"""
        df_feed = ch_get_df(query=query)
        return df_feed

    @task
    def extract_df_messanges():
        query = """WITH sent_messages AS (
                        SELECT user_id,
                            toDate(time) AS event_date,
                            count(user_id) AS messages_sent, 
                            count(DISTINCT reciever_id) AS users_received,
                            os, if(gender=0, 'male', 'female') as gender, age
                        FROM simulator_20230620.message_actions
                        where toDate(time) = today()-1
                        GROUP BY event_date, user_id, os, gender, age), 
                        received_messages AS (
                        SELECT reciever_id, count(reciever_id) AS messages_received, 
                               count(DISTINCT user_id) AS users_sent
                        FROM simulator_20230620.message_actions
                        where toDate(time) = today()-1
                        GROUP BY reciever_id)
                    SELECT event_date, user_id, 
                           messages_sent, messages_received, 
                           users_sent, users_received, 
                           os, gender, age
                    FROM sent_messages
                    JOIN received_messages 
                    ON sent_messages.user_id = received_messages.reciever_id;"""
        df_messanges = ch_get_df(query=query)
        return df_messanges

    @task
    def first_merge(df_feed, df_messanges):
        df_first_merge = df_feed.merge(
                            df_messanges, 
                            how='outer', 
                            on = ['event_date', 'user_id', 'gender', 'age', 'os'])\
                            .fillna(0)
        return df_first_merge

    @task
    def os_dimension(df_first_merge):
        df_os = df_first_merge\
                 .groupby(by=['event_date', 'os'], 
                           as_index=False)[['views',
                                            'likes',
                                            'messages_sent',
                                            'users_sent',
                                            'messages_received',
                                            'users_received']].sum()
        df_os['dimensions'] = 'os'
        df_os.rename(columns={'os': 'dimensions_value'}, inplace = True)
        df_os = df_os[['event_date'
                       , 'dimensions'
                       , 'dimensions_value'
                       , 'views'
                       , 'likes'
                       , 'messages_sent'
                       , 'messages_received'
                       , 'users_sent'
                       , 'users_received']]
        return df_os
    
    @task
    def gender_dimension(df_first_merge):
        df_gender = df_first_merge\
                 .groupby(by=['event_date', 'gender'], 
                           as_index=False)[['views',
                                            'likes',
                                            'messages_sent',
                                            'users_sent',
                                            'messages_received',
                                            'users_received']].sum()
        df_gender['dimensions'] = 'gender'
        df_gender.rename(columns={'gender': 'dimensions_value'}, inplace = True)
        df_gender = df_gender[['event_date'
                       , 'dimensions'
                       , 'dimensions_value'
                       , 'views'
                       , 'likes'
                       , 'messages_sent'
                       , 'messages_received'
                       , 'users_sent'
                       , 'users_received']]
        return df_gender
    
    @task
    def age_dimension(df_first_merge):
        df_age = df_first_merge\
                 .groupby(by=['event_date', 'age'], 
                           as_index=False)[['views',
                                            'likes',
                                            'messages_sent',
                                            'users_sent',
                                            'messages_received',
                                            'users_received']].sum()
        df_age['dimensions'] = 'age'
        df_age.rename(columns={'age': 'dimensions_value'}, inplace = True)
        df_age = df_age[['event_date'
                       , 'dimensions'
                       , 'dimensions_value'
                       , 'views'
                       , 'likes'
                       , 'messages_sent'
                       , 'messages_received'
                       , 'users_sent'
                       , 'users_received']]
        return df_age
    
    @task
    def merge_dimensions(df_gender, df_age, df_os):
        final_df = pd.concat([df_gender, df_age, df_os], axis=0)
        final_df[['views',
                    'likes',
                    'messages_sent',
                    'users_sent',
                    'messages_received',
                    'users_received']] = final_df[['views',
                                                'likes',
                                                'messages_sent',
                                                'users_sent',
                                                'messages_received',
                                                'users_received']].astype(int)
        final_df = final_df[['event_date',
                             'dimensions',
                             'dimensions_value',
                             'views',
                             'likes',
                             'messages_received',
                             'messages_sent',
                             'users_received',
                             'users_sent']]
        return final_df
    
    @task
    def ch_table(final_df):
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
                        'database':'test',
                        'user':'student-rw',
                        'password':'656e2b0c9c'}
        q = """CREATE TABLE IF NOT EXISTS test.khadchukaev
        (
            event_date Date,
            dimensions VARCHAR(30),
            dimensions_value VARCHAR(30),
            views Int64,
            likes Int64,
            messages_received Int64,
            messages_sent Int64,
            users_received Int64,
            users_sent Int64
            ) 
            ENGINE = MergeTree()
            order by event_date"""
        pandahouse.execute(query = q, 
                           connection = connection)
        pandahouse.to_clickhouse(df = final_df, 
                                 table = 'khadchukaev', 
                                 connection = connection, 
                                 index = False)

    df_feed = extract_df_feed()
    df_messanges = extract_df_messanges()
    df_first_merge = first_merge(df_feed, df_messanges)
    df_age = age_dimension(df_first_merge)
    df_gender = gender_dimension(df_first_merge)
    df_os = os_dimension(df_first_merge)
    final_df = merge_dimensions(df_gender, df_age, df_os)
    ch_table(final_df)

dag_khadchukaev = dag_khadchukaev()
