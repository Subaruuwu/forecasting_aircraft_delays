import time
from urllib.error import HTTPError
import logging
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, wait_incrementing
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from metar import Metar
import pandas as pd
import requests

def log_attempt_number(retry_state):
    """return the result of the last call attempt"""
    logging.error(f"Retrying: {retry_state.attempt_number}...")

class InAirportWeather:
    def __init__(self, icao, date_from, date_to):
        self.icao = icao
        self.date_from = pd.to_datetime(date_from)
        self.date_to = pd.to_datetime(date_to)
        self.url = f'https://www.ogimet.com/display_metars2.php?lang=en&lugar={self.icao}&tipo=SA&ord=REV&nil=SI&fmt=html'
        self.weather_data = pd.DataFrame(columns=['datetime', 'temperature', 'dew_point', 'wind_speed', 'wind_gust', 'wind_direction', 'peak_wind_direction', 'peak_wind_speed', 'visibility', 'pressure', 'weather', 'sky', 'sea_level_pressure', '1hr_precipitation'])

    def parse_weather(self):
        date_ranges = []
        temp_date_to = self.date_to
        while temp_date_to - self.date_from > pd.Timedelta(days=31):
            # self.look_at_url(self.date_from, temp_date_to + pd.Timedelta(days=31))
            date_ranges.append((self.date_from, temp_date_to + pd.Timedelta(days=31)))
            temp_date_to += pd.Timedelta(days=31)
        date_ranges.append((self.date_from, self.date_to))

        # with ThreadPoolExecutor() as executor:
        #     futures = [executor.submit(self.look_at_url, date_from, date_to) for date_from, date_to in date_ranges]
        #     for future in as_completed(futures):
        #         future.result()
        for range in date_ranges:
            self.look_at_url(range[0], range[1])



    @retry(wait=wait_incrementing(start=5, increment=5, max=100), retry=retry_if_exception_type(ConnectionAbortedError or HTTPError), after=log_attempt_number)
    def look_at_url(self, date_from, date_to):
        response = requests.get(self.url + f'&ano={date_from.year}&mes={date_from.month}&day={date_from.day}&hora=00&anof={date_to.year}&mesf={date_to.month}&dayf={date_to.day}&horaf=23&minf=59&send=send')
        response.raise_for_status()
        soup = BeautifulSoup(response.text, features='html.parser')
        metars = [el.get_text()[:-1] for el in soup.find_all('pre')]
        for i, row in enumerate(metars):
            obs = Metar.Metar(row)
            # self.weather_data = self.weather_data.append({'datetime': obs.time, 'temperature': obs.temp.value(), 'dew_point': obs.dewpt.value(), 'wind_speed': obs.wind_speed.value(), 'wind_gust': obs.wind_gust.value(), 'wind_direction': obs.wind_dir.value(), 'peak_wind_direction': obs.wind_dir_peak, 'peak_wind_speed': obs.wind_speed_peak, 'visibility': obs.vis.value(), 'pressure': obs.press.value(), 'weather': obs.weather, 'sky': obs.sky_conditions, 'sea_level_pressure': obs.press_sea_level, '1hr_precipitation': obs.precip_1hr}, ignore_index=True)
            if obs is not None:
                # print(i)
                datetime = pd.to_datetime(obs.time)
                if obs.temp != None:
                    temperature = obs.temp.value()
                else:
                    temperature = pd.NA

                if obs.dewpt != None:
                    dew_point = obs.dewpt.value()
                else:
                    dew_point = pd.NA

                if obs.wind_speed != None:
                    wind_speed = obs.wind_speed
                else:
                    wind_speed = pd.NA

                if obs.wind_gust != None:
                    wind_gust = obs.wind_gust
                else:
                    wind_gust = pd.NA

                if obs.wind_dir != None:
                    wind_direction = obs.wind_dir.value()
                else:
                    wind_direction = pd.NA

                if obs.wind_dir_peak != None:
                    peak_wind_direction = obs.wind_dir_peak.value()
                else:
                    peak_wind_direction = pd.NA

                if obs.wind_speed_peak != None:
                    peak_wind_speed = obs.wind_speed_peak.value()
                else:
                    peak_wind_speed = pd.NA

                if obs.vis != None:
                    visibility = obs.vis.value()
                else:
                    visibility = pd.NA

                if obs.press != None:
                    pressure = obs.press.value()
                else:
                    pressure = pd.NA

                if obs.weather != None:
                    weather = obs.weather
                else:
                    weather = pd.NA

                if obs.sky != None:
                    sky_conditions = [(i[0], int(i[1].value())) for i in obs.sky]
                else:
                    sky_conditions = pd.NA

                if obs.press_sea_level != None:
                    sea_level_pressure = obs.press_sea_level.value()
                else:
                    sea_level_pressure = pd.NA

                if obs.precip_1hr != None:
                    precipitation = obs.precip_1hr.value()
                else:
                    precipitation = pd.NA

                # temperature = lambda x: x.value() if obs.temp is not None else pd.NA
                # dew_point = lambda x: x.value() if obs.dewpt is not None else pd.NA
                # wind_speed = lambda x: x.value() if obs.wind_speed is not None else pd.NA
                # wind_gust = lambda x: x.value() if obs.wind_gust is not None else pd.NA
                # wind_direction = lambda x: x.value() if obs.wind_dir is not None else pd.NA
                # peak_wind_direction = lambda x: x.value() if obs.wind_dir_peak is not None else pd.NA
                # peak_wind_speed = lambda x: x.value() if obs.wind_speed_peak is not None else pd.NA
                # visibility = lambda x: x.value() if obs.vis is not None else pd.NA
                # pressure = lambda x: x.value() if obs.press is not None else pd.NA
                # weather = lambda x: x if obs.weather is not None else pd.NA
                # sky = lambda x: x[0], int(x[1].value) if obs.sky is not None else pd.NA
                # sea_level_pressure = lambda x: x.value() if obs.press_sea_level is not None else pd.NA
                # precipitation = lambda x: x if obs.precip_1hr is not None else pd.NA



                # print(f"datetime: {datetime}, temperature: {temperature(obs.temp)}, dew_point: {dew_point(obs.dewpt)}, wind_speed: {wind_speed(obs.wind_speed)}, wind_gust: {wind_gust(obs.wind_gust)}, wind_direction: {wind_direction(obs.wind_dir)}, peak_wind_direction: {peak_wind_direction(obs.wind_dir_peak)}, peak_wind_speed: {peak_wind_speed(obs.wind_speed_peak)}, visibility: {visibility(obs.vis)}, pressure: {pressure(obs.press)}, weather: {weather(obs.weather)}, sky: {sky(obs.sky_conditions)}, sea_level_pressure: {sea_level_pressure(obs.press_sea_level)}, precipitation: {precipitation(obs.precip_1hr)}")
                newline = pd.DataFrame({'datetime': [datetime], 'temperature': [temperature], 'dew_point': [dew_point], 'wind_speed': [wind_speed], 'wind_gust': [wind_gust], 'wind_direction': [wind_direction], 'peak_wind_direction': [peak_wind_direction], 'peak_wind_speed': [peak_wind_speed], 'visibility': [visibility], 'pressure': [pressure], 'weather': [weather], 'sky': [sky_conditions], 'sea_level_pressure': [sea_level_pressure], '1hr_precipitation': [precipitation]})
                self.weather_data = pd.concat([self.weather_data, newline], ignore_index=True)