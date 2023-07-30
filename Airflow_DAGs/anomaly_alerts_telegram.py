import pandas as pd
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta, date
import pandahouse
import io
import os
import sys
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

def check_anomaly(df, metric, threshold=0.25):
    # функция check_anomaly предлагает алгоритм проверки значения на аномальность посредством
    # сравнения интересующего значения со значением в это же время сутки назад
    # при желании алгоритм внутри этой функции можно изменить
    current_ts = df['ts'].max()  # достаем максимальную 15-минутку из датафрейма - ту, которую будем проверять на аномальность
    day_ago_ts = current_ts - pd.DateOffset(days=1)  # достаем такую же 15-минутку сутки назад
    week_ago_ts = current_ts - pd.DateOffset(days=7) # достаем такую же 15-минутку неделю назад

    current_value = df[df['ts'] == current_ts][metric].iloc[0] # достаем из датафрейма значение метрики в максимальную 15-минутку
    day_ago_value = df[df['ts'] == day_ago_ts][metric].iloc[0] # достаем из датафрейма значение метрики в такую же 15-минутку сутки назад
    week_ago_value = df[df['ts'] == week_ago_ts][metric].iloc[0] # достаем из датафрейма значение метрики в такую же 15-минутку неделю назад

    if current_value <= week_ago_value:
        diff_week = (current_value / week_ago_value - 1)
    else:
        diff_week = (week_ago_value / current_value - 1)

    if current_value <= day_ago_value:
        diff_day = (current_value / day_ago_value - 1)
    else:
        diff_day = (day_ago_value / current_value - 1)    
    
    # Проверяем наше значение на аномальность
    if (week_ago_value <= current_value <= day_ago_value) or (day_ago_value <= current_value <= week_ago_value):
        is_alert = 0
    elif abs(diff_week) >= threshold and abs(diff_day) >= threshold:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, current_value, diff_day, diff_week

schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, 
     schedule_interval=schedule_interval, 
     catchup=False)

def dag_khadchukaev_anomaly_alerts():
    @task
    def run_alerts(chat=None):
        chat_id = chat or ***
        bot = telegram.Bot(token='***')

        connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': 'simulator_20230620'}
        
        data = pandahouse.read_clickhouse(''' 
            with messages_table as (
                SELECT
                    toDate(ts) as date,
                    toStartOfFifteenMinutes(time) as ts,
                    formatDateTime(ts, '%R') as hm,
                    count(user_id) as messages,
                    uniqExact(user_id) as users_messages
                FROM simulator_20230620.message_actions
                WHERE ts >=  today() - 7 and ts < toStartOfFifteenMinutes(now())
                GROUP BY date, ts, hm
                ORDER BY ts),
            feed_table as (
                SELECT
                    toDate(ts) as date,
                    toStartOfFifteenMinutes(time) as ts,
                    formatDateTime(ts, '%R') as hm,
                    uniqExact(user_id) as users_feed,
                    countIf(action='like') as likes,
                    countIf(action='view') as views,
                    likes/views as ctr
                FROM simulator_20230620.feed_actions
                JOIN messages_table using ts
                WHERE ts >=  today() - 7 and ts < toStartOfFifteenMinutes(now())
                GROUP BY date, ts, hm
                ORDER BY ts)

            select date, ts, hm, users_feed, users_messages, likes, views, ctr, messages
            from messages_table
            join feed_table using ts ''', connection=connection)

        metrics = ['users_feed', 'users_messages', 'likes', 'views', 'ctr', 'messages']
        for metric in metrics:
            is_alert, current_value, diff_day, diff_week = check_anomaly(data, metric) # проверяем метрику на аномальность алгоритмом, описаным внутри функции check_anomaly()
            if is_alert:
                msg = '''Метрика {metric}:\nтекущее значение = {current_value:.2f}
                \nотклонение от вчера {diff_day:.2%}
                \nотклонение от недели назад {diff_week:.2%}
                \n Изучить подробнее на дашборде: {dashboard_link}'''.format(metric=metric,
                                                                current_value=current_value,
                                                                diff_day=diff_day,
                                                                diff_week=diff_week,
                                                                dashboard_link = 'https://superset.lab.karpov.courses/superset/dashboard/3990/')

                sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
                plt.tight_layout()
                today = pd.to_datetime(date.today())
                # Calculate the date 7 days ago
                seven_days_ago = today - timedelta(days=7)
                # Filter the data for dates that are either equal to 7 days ago or greater than or equal to today - 1 day
                filtered_data = data[(data['date'] == seven_days_ago) | (data['date'] >= today - timedelta(days=1))]
                ax = sns.lineplot( # строим линейный график
                    data=filtered_data.sort_values(by=['date', 'hm']), # задаем датафрейм для графика
                    x="hm", y=metric, # указываем названия колонок в датафрейме для x и y
                    hue="date" # задаем "группировку" на графике, т е хотим чтобы для каждого значения date была своя линия построена
                    )

                for ind, label in enumerate(ax.get_xticklabels()): # этот цикл нужен чтобы разрядить подписи координат по оси Х,
                    if ind % 15 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                ax.set(xlabel='time') # задаем имя оси Х
                ax.set(ylabel=metric) # задаем имя оси У

                ax.set_title('{}'.format(metric)) # задае заголовок графика
                ax.set(ylim=(0, None)) # задаем лимит для оси У

                # формируем файловый объект
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()
                # отправляем алерт
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
                
    try:
        run_alerts()
    except Exception as e:
        print(e)

dag_khadchukaev_anomaly_alerts = dag_khadchukaev_anomaly_alerts()