import kagglehub
import pandas as pd


class DataFrames:
    def __init__(self, flag_is_data_downloaded = False):
        self.flag_is_data_downloaded = flag_is_data_downloaded

        if self.flag_is_data_downloaded:

            path_flights = r'C:\Users\Mi\.cache\kagglehub\datasets\patrickzel\flight-delay-and-cancellation-dataset-2019-2023\versions\7\flights_sample_3m.csv'
            path_with_tail_number = r'C:\Users\Mi\.cache\kagglehub\datasets\deepankurk\flight-take-off-data-jfk-airport\versions\1\M1_final.csv'

            self.df_flights = pd.read_csv(path_flights)
            self.df_with_tail_number = pd.read_csv(path_with_tail_number)

        else:

            path_flights = kagglehub.dataset_download("patrickzel/flight-delay-and-cancellation-dataset-2019-2023")
            path_with_tail_number = kagglehub.dataset_download("deepankurk/flight-take-off-data-jfk-airport")

            self.flag_is_data_downloaded = True

            self.df_flights = pd.read_csv(path_flights)
            self.df_with_tail_number = pd.read_csv(path_with_tail_number)

    def get_df_flights(self):
        return self.df_flights

    def get_df_with_tail_number(self):
        return self.df_with_tail_number

    def get_info_df_flights(self):
        print("Общая информация о датасете df_flights:")
        return self.df_flights.info()

    def get_info_df_with_tail_number(self):
        print("Общая информация о датасете df_with_tail_number:")
        return self.df_with_tail_number.info()

    def clear_df(self, df: pd.DataFrame):
        df_cleared = df.dropna(axis=0, how='any', subset=None)
        return df_cleared