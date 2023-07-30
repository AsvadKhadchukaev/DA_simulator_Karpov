import pandas as pd
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import pandahouse
import io
import os
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

def dag_khadchukaev_full_report():

    @task
    def get_data():
        connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': '***',
        'user': '***',
        'database': 'simulator_20230620'}

        data_feed = pandahouse.read_clickhouse('''select toDate(time) as date , count (distinct user_id) as DAU_feed, 
                    countIf(action='like') as likes, countIf(action='view') as views,
                    views + likes as events,
                    uniqExact(post_id) as posts,
                    likes / DAU_feed as LPU,
                    likes/views as CTR
                    FROM simulator_20230620.feed_actions 
                    where toDate(time) >= today()-8 and toDate(time) <= today()-1
                    group by date
                    order by date''', connection=connection)
        
        data_msg = pandahouse.read_clickhouse('''select toDate(time) as date , count (distinct user_id) as DAU_msg, 
                    count(user_id) as msgs, msgs / DAU_msg as MPU
                    FROM simulator_20230620.message_actions 
                    where toDate(time) >= today()-8 and toDate(time) <= today()-1
                    group by date
                    order by date''', connection=connection)
        
        data_dau_all = pandahouse.read_clickhouse('''select date, 
                                        uniqExact(user_id) as users,
                                        uniqExactIf(user_id, os = 'iOS') as users_ios,
                                        uniqExactIf(user_id, os = 'Android') as users_android
                                from
                                    (select distinct toDate(time) as date, user_id, os
                                    FROM simulator_20230620.feed_actions
                                    where toDate(time) >= today()-8 and toDate(time) <= today()-1
                                    union all
                                    select distinct toDate(time) as date, user_id, os
                                    FROM simulator_20230620.message_actions
                                    where toDate(time) >= today()-8 and toDate(time) <= today()-1) as t
                                group by date
                                order by date''', connection=connection)
        
        data_new_users = pandahouse.read_clickhouse('''with t2 as (select user_id, min(start_date) as start_date
                                    from
                                    (select user_id, min(toDate(time)) as start_date
                                    from simulator_20230620.feed_actions
                                    where time between today()-100 and today()
                                    group by user_id
                                    union all
                                    select user_id, min(toDate(time)) as start_date
                                    from simulator_20230620.message_actions
                                    where time between today()-100 and today()
                                    group by user_id) t1
                                    group by user_id)
                                    
                                    select date,
                                        count(user_id) as new_users,
                                        uniqExactIf(user_id, source = 'organic') as new_users_organic,
                                        uniqExactIf(user_id, source = 'ads') as new_users_ads
                                    from
                                        (select distinct user_id, date, source, start_date
                                        from
                                            (select user_id, toDate(time) as date, source
                                            from simulator_20230620.feed_actions
                                            where time between today()-8 and today()
                                            union all
                                            select user_id, toDate(time) as date, source
                                            from simulator_20230620.message_actions
                                            where time between today()-8 and today()) t3
                                        join t2 using user_id
                                        where date = start_date) t4
                                    group by date
                                    order by date''', connection=connection)

        data_new_users['date'] = pd.to_datetime(data_new_users['date']).dt.date
        data_dau_all['date'] = pd.to_datetime(data_dau_all['date']).dt.date
        data_msg['date'] = pd.to_datetime(data_msg['date']).dt.date
        data_feed['date'] = pd.to_datetime(data_feed['date']).dt.date
        data_new_users = data_new_users.astype({'new_users':int, 'new_users_organic': int, 'new_users_ads':int})
        data_dau_all = data_dau_all.astype({'users':int, 'users_ios': int, 'users_android':int})
        data_msg = data_msg.astype({'DAU_msg':int, 'msgs': int})
        data_feed = data_feed.astype({'DAU_feed':int, 'views': int, 'likes': int, 'events': int, 'posts': int})
        data = {'data_new_users': data_new_users,
        'data_dau_all': data_dau_all,
        'data_msg': data_msg,
        'data_feed': data_feed}

        return data
    
    @task
    def get_plot(data, chat = None):

        data_feed = data['data_feed']
        data_msg = data['data_msg']
        data_new_users = data['data_new_users']
        data_dau_all = data['data_dau_all']

        chat_id = chat or -867652742
        bot = telegram.Bot(token = '6272504639:AAFk_LUI76DQaNOkvglYdzE4PclM0edmoo0')

        data = pd.merge(data_feed, data_msg, on='date')
        data = pd.merge(data, data_new_users, on='date')
        data = pd.merge(data, data_dau_all, on='date')

        data['events_all'] = data['events'] + data['msgs']
        
        sns.set()
        fig, axes = plt.subplots(3, figsize = (10, 14))
        fig.suptitle('Статистика по всему приложению за 7 дней')
        app_dict = {0:{'y':['events_all'], 'title':'Events'},
                    1:{'y':['users', 'users_ios', 'users_android'], 'title':'DAU'},
                    2:{'y':['new_users', 'new_users_ads', 'new_users_organic'], 'title':'New users'}}
        
        for plot in range(3):
            for y in app_dict[plot]['y']:
                sns.lineplot(ax = axes[plot], data=data, x='date', y=y)
            axes[plot].set_title(app_dict[(plot)]['title'])
            axes[plot].set(xlabel=None)
            axes[plot].set(ylabel=None)
            for ind, label in enumerate(axes[plot].get_xticklabels()):
                if ind % 3 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'app_stat.png'
        plot_object.seek(0)
        plt.close
        bot.send_photo(chat_id=chat_id, photo=plot_object)

    @task
    def app_report(data, chat=None):

        chat_id = chat or ***
        bot = telegram.Bot(token = '***')
        msg = '''
        Отчет по всему приложению за {date}
        Всего событий: {events}
        🙍‍♂️🙍‍♀️DAU: {users} ({to_users_day_ago:+.2%} к дню назад {to_users_week_ago:+.2%} к неделе назад)
        🙍‍♂️🙍‍♀️DAU по платформам:
            🍏ios: {users_ios} ({to_users_ios_day_ago:+.2%} к дню назад {to_users_ios_week_ago:+.2%} к неделе назад)
            🤖Android: {users_android} ({to_users_android_day_ago:+.2%} к дню назад {to_users_android_week_ago:+.2%} к неделе назад)
        👶Новые пользователи: {new_users} ({to_new_users_day_ago:+.2%} к дню назад {to_new_users_week_ago:+.2%} к неделе назад)
        👶Новые польззователи по источнику:
            🌿Органические пользователи: {new_users_organic} ({to_new_users_organic_day_ago:+.2%} к дню назадб {to_new_users_organic_week_ago:+.2%} к неделе назад)
            💵Реклама: {new_users_ads} ({to_new_users_ads_day_ago:+.2%} к дню назад {to_new_users_ads_week_ago:+2%} к неделе назад)
        ❤️Лайки: {likes} ({to_likes_day_ago:+.2%} к дню назад {to_likes_week_ago:+.2%} к неделе назад)
        👀Просмотры: {views} ({to_views_day_ago:+.2%} к дню назад {to_views_week_ago:+.2%} к неделе назад)
        ✉️Сообщения по пользователям: {mpu:.3} ({to_mpu_day_ago:+.2%} к дню назад {to_mpu_week_ago:+.2%} к неделе назад)
        '''

        today = pd.Timestamp('now') - pd.DateOffset(days=1)
        day_ago = today - pd.DateOffset(days=1)
        week_ago = today - pd.DateOffset(days=7)

        data_feed = data['data_feed']
        data_msg = data['data_msg']
        data_new_users = data['data_new_users']
        data_dau_all = data['data_dau_all']


        data_new_users['date'] = pd.to_datetime(data_new_users['date']).dt.date
        data_dau_all['date'] = pd.to_datetime(data_dau_all['date']).dt.date
        data_msg['date'] = pd.to_datetime(data_msg['date']).dt.date
        data_feed['date'] = pd.to_datetime(data_feed['date']).dt.date
        data_new_users = data_new_users.astype({'new_users':int, 'new_users_organic': int, 'new_users_ads':int})
        data_dau_all = data_dau_all.astype({'users':int, 'users_ios': int, 'users_android':int})
        data_msg = data_msg.astype({'DAU_msg':int, 'msgs': int})
        data_feed = data_feed.astype({'DAU_feed':int, 'views': int, 'likes': int, 'events': int, 'posts': int})

        report = msg.format(date = today.date(), 
                            events = data_msg[data_msg['date'] == today.date()]['msgs'].iloc[0]
                            + data_feed[data_feed['date'] == today.date()]['events'].iloc[0], 
                            users = data_dau_all[data_dau_all['date'] == today.date()]['users'].iloc[0],
                            to_users_day_ago = (data_dau_all[data_dau_all['date'] == today.date()]['users'].iloc[0]
                                            - data_dau_all[data_dau_all['date'] == day_ago.date()]['users'].iloc[0]) 
                                            / data_dau_all[data_dau_all['date'] == day_ago.date()]['users'].iloc[0],
                            to_users_week_ago = (data_dau_all[data_dau_all['date'] == today.date()]['users'].iloc[0]
                                            - data_dau_all[data_dau_all['date'] == week_ago.date()]['users'].iloc[0]) 
                                            / data_dau_all[data_dau_all['date'] == week_ago.date()]['users'].iloc[0],
                            users_ios = data_dau_all[data_dau_all['date'] == today.date()]['users_ios'].iloc[0],
                            to_users_ios_day_ago = (data_dau_all[data_dau_all['date'] == today.date()]['users_ios'].iloc[0]
                                            - data_dau_all[data_dau_all['date'] == day_ago.date()]['users_ios'].iloc[0]) 
                                            / data_dau_all[data_dau_all['date'] == day_ago.date()]['users_ios'].iloc[0],
                            to_users_ios_week_ago = (data_dau_all[data_dau_all['date'] == today.date()]['users_ios'].iloc[0]
                                            - data_dau_all[data_dau_all['date'] == week_ago.date()]['users_ios'].iloc[0]) 
                                            / data_dau_all[data_dau_all['date'] == week_ago.date()]['users_ios'].iloc[0],
                            users_android = data_dau_all[data_dau_all['date'] == today.date()]['users_android'].iloc[0],
                            to_users_android_day_ago = (data_dau_all[data_dau_all['date'] == today.date()]['users_android'].iloc[0]
                                            - data_dau_all[data_dau_all['date'] == day_ago.date()]['users_android'].iloc[0]) 
                                            / data_dau_all[data_dau_all['date'] == day_ago.date()]['users_android'].iloc[0],
                            to_users_android_week_ago = (data_dau_all[data_dau_all['date'] == today.date()]['users_android'].iloc[0]
                                            - data_dau_all[data_dau_all['date'] == week_ago.date()]['users_android'].iloc[0]) 
                                            / data_dau_all[data_dau_all['date'] == week_ago.date()]['users_android'].iloc[0],
                            new_users = data_new_users[data_new_users['date'] == today.date()]['new_users'].iloc[0],
                            to_new_users_day_ago = (data_new_users[data_new_users['date'] == today.date()]['new_users'].iloc[0]
                                            - data_new_users[data_new_users['date'] == day_ago.date()]['new_users'].iloc[0]) 
                                            / data_new_users[data_new_users['date'] == day_ago.date()]['new_users'].iloc[0],
                            to_new_users_week_ago = (data_new_users[data_new_users['date'] == today.date()]['new_users'].iloc[0]
                                            - data_new_users[data_new_users['date'] == week_ago.date()]['new_users'].iloc[0]) 
                                            / data_new_users[data_new_users['date'] == week_ago.date()]['new_users'].iloc[0],
                            new_users_ads = data_new_users[data_new_users['date'] == today.date()]['new_users_ads'].iloc[0],
                            to_new_users_ads_day_ago = (data_new_users[data_new_users['date'] == today.date()]['new_users_ads'].iloc[0]
                                            - data_new_users[data_new_users['date'] == day_ago.date()]['new_users_ads'].iloc[0]) 
                                            / data_new_users[data_new_users['date'] == day_ago.date()]['new_users_ads'].iloc[0],
                            to_new_users_ads_week_ago = (data_new_users[data_new_users['date'] == today.date()]['new_users_ads'].iloc[0]
                                            - data_new_users[data_new_users['date'] == week_ago.date()]['new_users_ads'].iloc[0]) 
                                            / data_new_users[data_new_users['date'] == week_ago.date()]['new_users_ads'].iloc[0],
                            new_users_organic = data_new_users[data_new_users['date'] == today.date()]['new_users_organic'].iloc[0],
                            to_new_users_organic_day_ago = (data_new_users[data_new_users['date'] == today.date()]['new_users_organic'].iloc[0]
                                            - data_new_users[data_new_users['date'] == day_ago.date()]['new_users_organic'].iloc[0]) 
                                            / data_new_users[data_new_users['date'] == day_ago.date()]['new_users_organic'].iloc[0],
                            to_new_users_organic_week_ago = (data_new_users[data_new_users['date'] == today.date()]['new_users_organic'].iloc[0]
                                            - data_new_users[data_new_users['date'] == week_ago.date()]['new_users_organic'].iloc[0]) 
                                            / data_new_users[data_new_users['date'] == week_ago.date()]['new_users_organic'].iloc[0],
                            likes = data_feed[data_feed['date'] == today.date()]['likes'].iloc[0],
                            to_likes_day_ago = (data_feed[data_feed['date'] == today.date()]['likes'].iloc[0]
                                            - data_feed[data_feed['date'] == day_ago.date()]['likes'].iloc[0])
                                            / data_feed[data_feed['date'] == day_ago.date()]['likes'].iloc[0],
                            to_likes_week_ago = (data_feed[data_feed['date'] == today.date()]['likes'].iloc[0]
                                            - data_feed[data_feed['date'] == week_ago.date()]['likes'].iloc[0])
                                            / data_feed[data_feed['date'] == week_ago.date()]['likes'].iloc[0],
                            views = data_feed[data_feed['date'] == today.date()]['views'].iloc[0],
                            to_views_day_ago = (data_feed[data_feed['date'] == today.date()]['views'].iloc[0]
                                            - data_feed[data_feed['date'] == day_ago.date()]['views'].iloc[0])
                                            / data_feed[data_feed['date'] == day_ago.date()]['views'].iloc[0],
                            to_views_week_ago = (data_feed[data_feed['date'] == today.date()]['views'].iloc[0]
                                            - data_feed[data_feed['date'] == week_ago.date()]['views'].iloc[0])
                                            / data_feed[data_feed['date'] == week_ago.date()]['views'].iloc[0],
                            mpu = data_msg[data_msg['date'] == today.date()]['MPU'].iloc[0],
                            to_mpu_day_ago = (data_msg[data_msg['date'] == today.date()]['MPU'].iloc[0]
                                            - data_msg[data_msg['date'] == day_ago.date()]['MPU'].iloc[0])
                                            / data_msg[data_msg['date'] == day_ago.date()]['MPU'].iloc[0],
                            to_mpu_week_ago = (data_msg[data_msg['date'] == today.date()]['MPU'].iloc[0]
                                            - data_msg[data_msg['date'] == week_ago.date()]['MPU'].iloc[0])
                                            / data_msg[data_msg['date'] == week_ago.date()]['MPU'].iloc[0])

        bot.send_message(chat_id=chat_id, text=report)

    data = get_data()
    
    app_report(data)
    get_plot(data)

dag_khadchukaev_full_report = dag_khadchukaev_full_report()