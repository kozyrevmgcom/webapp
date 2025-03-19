import streamlit as st
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException
import pandas as pd
import time
from datetime import date, timedelta

# загружаем переменные виртуального окружения


def get_result(client_name, tracker_impressions, tracker_conversions, size):
    # Устанавливаем параметры подключения
    ch_client = Client(
                    host=st.secrets["CH_HOST"],  
                    port=9440,   
                    user=st.secrets["CH_USER"],   
                    password=st.secrets["CH_PASSWORD"],
                    database=st.secrets["CH_DATABASE"],
                    secure=True, 
                    verify=False
                    )
    

    # Получаем структуру таблицы (названия колонок)
    columns = [col[0] for col in ch_client.execute(f'DESCRIBE TABLE {client_name}_{tracker_impressions}')]

    # Выполняем запрос
    # result = ch_client.execute(f'SELECT * FROM {client_name}_{tracker_impressions} LIMIT 10')

    first_date = date_range[0].strftime("%Y-%m-%d")
    second_date = date_range[1].strftime("%Y-%m-%d")
    third_date = date_range[1] + timedelta(days=size)


    query = f"""
        SELECT
            advertising_id,
            event_time,
            last_interaction as date,
            customs_string,
            ad_name,
            event_name,
            event_value,
            ABS(DATEDIFF('day', event_time, last_interaction)) as time_to_conversion 
        FROM 
            

            (SELECT
                    c.advertising_id,
                    c.event_time,
                    a.datetime as last_interaction,
                    ROW_NUMBER() OVER(PARTITION BY c.advertising_id, c.event_time ORDER BY a.datetime DESC) as win,
                    a.customs_string,
                    a.ad_name,
                    c.event_name,
                    c.event_value
                FROM db1.{client_name}_{tracker_conversions} as c
                JOIN db1.{client_name}_{tracker_impressions} as a ON c.advertising_id = a.advertising_id
                WHERE datetime BETWEEN '{first_date}' AND '{second_date}' AND
                event_time BETWEEN '{first_date}' AND '{third_date}' AND
                (toUnixTimestamp(c.event_time) - toUnixTimestamp(a.datetime)) BETWEEN 0 AND {size}*24*60*60
                GROUP BY
                    c.advertising_id,
                    c.event_time,
                    last_interaction,
                    a.customs_string,
                    a.ad_name,
                    c.event_name,
                    c.event_value)
        WHERE win = 1
        ORDER BY event_time"""
    
    result = ch_client.execute(query, with_column_types=True) 

    data = [row for row in result[0]]  # Данные
    columns = [col[0] for col in result[1]]  # Названия колонок

    # Преобразуем в DataFrame
    df = pd.DataFrame(data, columns=columns)
    return df


def execute_query(client_name, tracker_impressions, tracker_conversions, size):

    latest_iteration = st.empty()
    bar = st.progress(0)
    
    for i in range(100):
        latest_iteration.text(f'Загрузка... {i+1}%')
        bar.progress(i + 1)
        time.sleep(0.02)
    
    df = get_result(client_name, tracker_impressions, tracker_conversions, size)
    
    latest_iteration.empty()
    bar.empty()

    return df

# Выбор клиента
client_options = ["hoff", "rendezv"]
client_name = st.sidebar.selectbox("Выберите клиента", client_options)

# Выбор трекера
tracker_impressions_options = {"hoff": ["adriver", "hybe"], "rendezv": ["adriver"]}
tracker_impressions = st.sidebar.selectbox("Выберите трекер показов", tracker_impressions_options[client_name])

# Выбор трекера
tracker_conversions_options = {"hoff": ["appsflyer"], "rendezv": ["appsflyer"]}
tracker_conversions = st.sidebar.selectbox("Выберите трекер конверсий", tracker_conversions_options[client_name])


# Выбор даты в режиме диапазона
date_range = st.sidebar.date_input(
    "Выберите период рекламной кампании",
    value=(date.today(), date.today()),  # Значение по умолчанию (сегодня)
    min_value=date(2025, 1, 1),  # Минимальная возможная дата
    max_value=date.today(),  # Максимальная возможная дата
)




# Разбираем кортеж (start_date, end_date)
if isinstance(date_range, tuple) and len(date_range) == 2:
    pass
else:
    st.write("Пожалуйста, выберите обе даты.")

# Окно аттрибуции
days_to_add = st.sidebar.number_input("Окно атрибуции", min_value=0, step=1)

# Инициализация session_state, если df еще не существует
if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame()  # Создаем пустой DataFrame

if st.sidebar.button("🔄 Выполнить"):
    df = execute_query(client_name, tracker_impressions, tracker_conversions, days_to_add)
        # Если есть данные, отображаем таблицу и кнопку скачивания
    if st.session_state.df is not None:

        if df.empty:
            st.write("Нет данных.")
        else:
            st.write("### Результаты запроса")
            st.write(f"Конверсий: {df.shape[0]}")
            st.dataframe(df)  # Выводим DataFrame в Streamlit
            # Преобразуем DataFrame в CSV
            csv = df.to_csv(index=False)

            # Кнопка для скачивания
            st.download_button(
                label="📥 Скачать данные",
                data=csv,
                file_name="data.csv",
                mime="text/csv"
                )


