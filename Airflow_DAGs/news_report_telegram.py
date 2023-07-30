import telegram
import pandahouse
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
sns.__version__ = '0.12.2'
import io
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a-hadchukaev-20',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 3, 10),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, 
     schedule_interval=schedule_interval, 
     catchup=False)

def dag_khadchukaev_bot_report():

    @task
    def load_dataframe(): 
        connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': 'dpo_python_2020',
        'user': 'student',
        'database': 'simulator'}
        q = """select toDate(time) as date , count (distinct user_id) as DAU, 
                countIf(action='like') as likes, countIf(action='view') as views,
                likes/views as CTR
                FROM simulator_20230620.feed_actions 
                where toDate(time) >= today()-7 and toDate(time) <= today()-1
                group by date"""
        df = pandahouse.read_clickhouse(q, connection=connection)
        print(df.head(2))
        return df
    
    @task
    def text(df):
        df_yesterday = df[df.date == df.date.max()]
        yesterday = df_yesterday.date.iloc[0].date()
        dau = df_yesterday.DAU.iloc[0]
        views = df_yesterday.views.iloc[0]
        likes = df_yesterday.likes.iloc[0]
        ctr =df_yesterday.CTR.iloc[0]
        ctr = str(ctr.round(6)*100)+'%'
        text = f'Ключевые метрики за {yesterday}:\nDAU: {dau}\nПросмотры: {views}\nЛайки: {likes}\nCTR: {ctr}'
        
        return text

    @task
    def graph(df, metric):
        plt.figure(figsize=(8, 4))
        ax = sns.lineplot(x=df['date'], y=df[metric])
        plt.title(f'{metric}')
        ax.set(xlabel=None, ylabel=None)

        # Save the plot to a BytesIO buffer
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)

        # Clear the current figure to release resources
        plt.clf()
        plt.close()

        return buffer

    @task
    def send_message(text, photos):
        my_token = '6272504639:AAFk_LUI76DQaNOkvglYdzE4PclM0edmoo0'
        chat_id=-867652742
        bot = telegram.Bot(token=my_token)

        bot.sendMessage(chat_id=chat_id, text=text)
        for photo in photos:
            bot.sendPhoto(chat_id=chat_id, photo=photo)

    df = load_dataframe()
    text = text(df)
    photo_dau = graph(df, metric = 'DAU')
    photo_likes = graph(df, metric = 'likes')
    photo_views = graph(df, metric = 'views')
    photo_CTR = graph(df, metric = 'CTR')
    photos = [photo_dau, photo_likes, photo_views, photo_CTR]
    send_message(text, photos)

dag_khadchukaev_bot_report = dag_khadchukaev_bot_report()

