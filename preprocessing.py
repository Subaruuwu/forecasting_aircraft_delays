import pandas as pd
import pytz
from datetime import datetime, timedelta
import numpy as np

# махинации с дататайпами
def to_int_ignore_none(num):
    if num is None or pd.isnull(num):
        return None
    return int(num)

def convert_utc_to_jfk(df):
    """
    Конвертирует время из UTC в UTC-5 с учётом летнего и зимнего времени для датасета с погодой METAIR
    """
    # Указываем часовой пояс для UTC и Нью-Йорка
    utc = pytz.utc
    ny_tz = pytz.timezone("America/New_York")

    # Создаем новые столбцы DATE_JFK и TIME_JFK
    df['DATE_JFK'] = None
    df['TIME_JFK'] = None

    # Преобразуем столбец 'valid' из строки в объект datetime
    df.loc[:, 'valid'] = pd.to_datetime(df.loc[:, 'valid'])

    # Переводим время из UTC в местное для Нью-Йорка
    for index, row in df.iterrows():
        # Интерпретация времени в UTC
        utc_time = utc.localize(row['valid'])

        # Перевод в часовой пояс Нью-Йорка
        ny_time = utc_time.astimezone(ny_tz)

        # Разделение на дату и время
        df.at[index, 'DATE_JFK'] = ny_time.date()
        df.at[index, 'TIME_JFK'] = ny_time.time()

    return df


def merge_weather_with_flights(df_flights: pd.DataFrame, data_enhanced: pd.DataFrame) -> pd.DataFrame:
    """
    Добавляет погодные данные из data_enhanced к рейсам в df_flights.

    Погодные данные выбираются на основе совпадения даты и ближайшего времени (не более чем на 5 минут позже запланированного времени вылета).
    """
    # Преобразуем время в удобный формат
    data_enhanced.loc[:, 'DATE_JFK'] = pd.to_datetime(data_enhanced.loc[:, 'DATE_JFK'])
    data_enhanced.loc[:, 'TIME_JFK'] = pd.to_timedelta(data_enhanced.loc[:, 'TIME_JFK'].astype(str))

    # Преобразуем дату и время в df_flights
    df_flights.loc[:, 'FL_DATE'] = pd.to_datetime(df_flights.loc[:, 'FL_DATE'])

    def convert_crs_dep_time(value):
        """
        Преобразует время CRS_DEP_TIME из формата hhmm различной длины в timedelta.
        """
        value_str = str(value)
        if len(value_str) == 1 or len(value_str) == 2:
            # Если длина 1 или 2, это минуты
            hours = 0
            minutes = int(value_str)
        elif len(value_str) == 3:
            # Если длина 3, первая цифра - часы, остальные - минуты
            hours = int(value_str[0])
            minutes = int(value_str[1:])
        elif len(value_str) == 4:
            # Если длина 4, первые две цифры - часы, остальные - минуты
            hours = int(value_str[:2])
            minutes = int(value_str[2:])
        else:
            raise ValueError("Неправильный формат CRS_DEP_TIME")
        return timedelta(hours=hours, minutes=minutes)

    df_flights.loc[:, 'CRS_DEP_TIME'] = df_flights.loc[:, 'CRS_DEP_TIME'].apply(convert_crs_dep_time)

    # Создаём копию для объединения
    enriched_flights = df_flights.copy()

    # Список колонок, которые нужно добавить из data_enhanced
    weather_columns = data_enhanced.columns.difference(['DATE_JFK', 'TIME_JFK'])

    # Создаём пустые колонки для погоды в enriched_flights
    for col in weather_columns:
        enriched_flights[col] = None

    # Проходим по всем строкам df_flights
    for index, flight in df_flights.iterrows():
        # Фильтруем data_enhanced по дате ?? bruh
        same_date_weather = data_enhanced[data_enhanced['DATE_JFK'] == flight['FL_DATE']]

        if not same_date_weather.empty:
            # Рассчитываем разницу по времени
            same_date_weather['TIME_DIFF'] = abs(same_date_weather['TIME_JFK'] - flight['CRS_DEP_TIME'])

            # Оставляем строки, где TIME_JFK не более чем на 5 минут больше CRS_DEP_TIME
            valid_weather = same_date_weather[same_date_weather['TIME_JFK'] <= (flight['CRS_DEP_TIME'] + timedelta(minutes=5))]

            if not valid_weather.empty:
                # Берём ближайшую запись
                closest_weather = valid_weather.loc[valid_weather['TIME_DIFF'].idxmin()]

                # Копируем погодные данные в enriched_flights
                for col in weather_columns:
                    enriched_flights.at[index, col] = closest_weather[col]

    return enriched_flights

def merge_with_tail_number(df_flights: pd.DataFrame, df_tails: pd.DataFrame):
    """
    Добавляет данные о самолёте из df_tails к рейсам в df_flights.
    """
    # Создаём копию для объединения
    enriched_flights = df_flights.copy()

    # Преобразование даты, иначе ошибки (лол)
    df_flights.loc[:, 'DATE_CONVERTED'] = pd.to_datetime(df_flights.loc[:, 'FL_DATE'])

    # Выбор даты между 2019-12-01 и 2020-11-30
    df_flights = df_flights[(df_flights['DATE_CONVERTED'] >= pd.to_datetime('2019-11-01')) & (df_flights['DATE_CONVERTED'] <= pd.to_datetime('2020-11-30'))]

    # Создаём столбец для объединения
    df_flights['merging_column'] = None
    df_tails['merging_column'] = None

    # Заполняем столбец для объединения
    for idx, row in df_flights.iterrows():
        df_flights.loc[idx, 'merging_column'] = str(row['DATE_CONVERTED'].month) + str(row['DATE_CONVERTED'].day) + row['AIRLINE_CODE'] + row['DEST'] + str(to_int_ignore_none(row['TAXI_OUT']))

    for idx, row in df_tails.iterrows():
        df_tails.loc[idx, 'merging_column'] = str(row['MONTH']) + str(row['DAY_OF_MONTH']) + row['OP_UNIQUE_CARRIER'] + row['DEST'] + str(to_int_ignore_none(row['TAXI_OUT']))

    # Выбираем нужные столбцы
    temp_df = df_tails.loc[:, ['merging_column', 'TAIL_NUM']]

    # Объединяем данные
    merged_data = pd.merge(df_flights, temp_df, on='merging_column')

    # Дроп временных столбцов
    merged_data.drop(columns=['DATE_CONVERTED', 'merging_column'], inplace=True)

    return merged_data

def get_aircraft_age(data: pd.DataFrame):
    """
    Получает возраст самолёта на момент вылета.
    """

    age_data = pd.read_csv('MASTER.txt')
    age_data = age_data.loc[:, ['N-NUMBER', 'YEAR MFR']]

    # Преобразование даты, иначе ошибки (лол)
    data.loc[:, 'DATE_CONVERTED'] = pd.to_datetime(data.loc[:, 'FL_DATE'])

    if 'TAIL_NUM' not in data.columns:
        raise ValueError("Данные о самолёте не найдены")

    data.loc[:, 'AGE'] = None

    for idx, row in data.iterrows():
        tail_num = row['TAIL_NUM'][1:]
        date = row['DATE_CONVERTED']

        aircraft = age_data.loc[age_data['N-NUMBER'] == tail_num]

        if not aircraft.empty:
            year = aircraft['YEAR MFR'].values[0]
            if not pd.isnull(year) and year.isnumeric():
                age = date.year - to_int_ignore_none(year)
                data.loc[idx, 'AGE'] = age

    return data

