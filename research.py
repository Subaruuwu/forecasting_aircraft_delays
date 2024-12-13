import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import LabelEncoder
from typing import Optional


def plot_cancelled_flights(df, cols_type=1):
    """
    Рисует график отмены рейсов по разным причинам по месяцам и годам.
    Флаг cols_type нужен потому что масштаб разных причин разный и не позволяет нормально проанализировать график.
    :param df:
    :param cols_type: 1 - используются все причины без причин безопасности, 0 - только по причинам безопасности
    :return:
    """
    cancellation_code_description = {
        'A': 'по причинам авиакомпании',
        'B': 'по причинам погоды',
        'C': 'по причинам национальной авиационной системы',
        'D': 'по причинам безопасности'
    }
    cancelled_flights = df[df['CANCELLED'] == 1]
    if cols_type:
        cancelled_flights_withoud_d_reason = cancelled_flights[cancelled_flights['CANCELLATION_CODE'] != 'D']
    else:
        cancelled_flights_withoud_d_reason = cancelled_flights[cancelled_flights['CANCELLATION_CODE'] == 'D']
    cancellations_by_reason = (
        cancelled_flights_withoud_d_reason.groupby(['Year', 'Month', 'CANCELLATION_CODE'])
        .size()
        .reset_index(name='Count')
    )
    cancellations_by_reason['CANCELLATION_CODE'] = cancellations_by_reason['CANCELLATION_CODE'].apply(
        lambda x: cancellation_code_description[x])
    plt.figure(figsize=(16, 8))
    sns.lineplot(
        data=cancellations_by_reason,
        x='Month',
        y='Count',
        hue='CANCELLATION_CODE',
        style='Year',
        markers=True
    )
    plt.title("Отмена рейсов по причинам по месяцам и годам")
    plt.xlabel("Месяц")
    plt.ylabel("Количество отмен")
    plt.xticks(range(1, 13))
    plt.grid()
    plt.legend(title="Причина отмены (код)")


def plot_hot_map_with_dep_delay(df, flag_cols_type='object', columns: Optional[list] = None):
    """
    Рисует горячую карту корреляции между признаками и целевой переменной DEP_DELAY
    :param df:
    :param flag_cols_type: 'object' - используются все признаки, 'numeric' - только числовые признаки
    :param columns: список признаков для которых строится корреляция. используется для flag_cols_type='numeric'.
    """

    if columns is not None:
        df_encoded = df[columns].copy()
        # df = df[columns].copy()
    else:
        df_encoded = df.copy()
    label_encoders = {}

    if flag_cols_type == 'object':
        for col in df.select_dtypes(include=['object']).columns:
            le = LabelEncoder()
            df_encoded[col] = le.fit_transform(df[col].astype(str))
            label_encoders[col] = le  # Сохраняем encoder для возможного обратного преобразования

        correlation = df_encoded.corr()

    elif flag_cols_type == 'numeric':
        correlation = df[columns].select_dtypes(include='number').corr()

    correlation_with_target = correlation['DEP_DELAY'].sort_values(ascending=False)

    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation[['DEP_DELAY']], annot=True, fmt=".2f", cmap="coolwarm", cbar=True)
    plt.title("Корреляция между признаками и DEP_DELAY")
    plt.xlabel("Целевая переменная (DEP_DELAY)")
    plt.ylabel("Признаки")
    plt.show()


def plot_average_delay_due_different_reasons(df):
    if 'FL_DATE' in df.columns:
        # Преобразование столбца с датой
        df['FL_DATE'] = pd.to_datetime(df['FL_DATE'])
        df['Month'] = df['FL_DATE'].dt.month
        df['Year'] = df['FL_DATE'].dt.year

        # Список причин задержек и соответствующих столбцов
        delay_columns = {
            'Задержка из-за погоды': 'DELAY_DUE_WEATHER',
            'Задержка из-за авиакомпании': 'DELAY_DUE_CARRIER',
            'Задержка из-за безопасности': 'DELAY_DUE_SECURITY',
            'Задержка из-за NAS': 'DELAY_DUE_NAS',
            'Задержка из-за позднего прибытия самолёта': 'DELAY_DUE_LATE_AIRCRAFT',
        }

        for reason, column in delay_columns.items():
            if column in df.columns:
                # Группировка данных
                monthly_delays = (
                    df.groupby(['Year', 'Month'])[column]
                    .mean()
                    .reset_index()
                )

                # Построение графика
                sns.lineplot(
                    data=monthly_delays,
                    x='Month',
                    y=column,
                    hue='Year',
                    marker='o'
                )
                plt.title(f"Среднее время задержки по месяцам и годам ({reason})")
                plt.xlabel("Месяц")
                plt.ylabel("Средняя задержка (минуты)")
                plt.xticks(range(1, 13))  # Отображение месяцев 1-12
                plt.grid()
                plt.show()