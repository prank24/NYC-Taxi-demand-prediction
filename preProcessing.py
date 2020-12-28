import datetime
import csv
from asyncio import sleep
from typing import List, Optional, Union
from os import listdir, remove, mkdir, chdir
from os.path import isfile, join, isdir

from pandas import DataFrame, to_datetime, read_csv
from wwo_hist import retrieve_hist_data

AnyPath = Union[str, bytes]


class PreProcessor:
    __input_fileNames: List[str] = []
    __output_files: List[str] = []

    def __init__(self, input_path: str, output_path: str):
        self._input_path = input_path
        self._output_path = output_path

    def get_all_input_csv(self) -> List[str]:
        return self.__input_fileNames

    def get_all_output_csv(self) -> List[str]:
        return self.__output_files

    def write_dataframe_as_csv(self, data: DataFrame, name: str):
        chdir(self._output_path)
        print("Writing CSV to Path {}", name + ".csv")
        data.to_csv(name + ".csv")

    @staticmethod
    def remove_dir_if_exist(dir_path: AnyPath):
        if isdir(dir_path):
            print("Deleting content on {}".format(dir_path))
            remove(dir_path)

    @staticmethod
    def make_dir(dir_path: AnyPath):
        # PreProcessor.remove_dir_if_exist(dir_path)
        mkdir(dir_path)

    @staticmethod
    def get_code_for_time(date_time: datetime) -> int:
        hour = date_time.hour
        code = 1
        if hour != 0:
            code = (hour * 2) + 1
        if date_time.minute >= 30:
            code = code + 1
        return code

    @staticmethod
    def last_day_of_month(any_day: datetime) -> datetime:
        # this will never fail
        # get close to the end of the month for any day, and add 4 days 'over'
        next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
        # subtract the number of remaining 'overage' days to get last day of current month,
        # or said programattically said, the previous day of the first of next month
        return next_month - datetime.timedelta(days=next_month.day)

    @staticmethod
    def first_day_of_month(month: int, year: int) -> datetime:
        return datetime.datetime(year, month, 1)

    def list_files(self):
        self.__input_fileNames = [f for f in listdir(self._input_path)
                                  if (isfile(join(self._input_path, f)) and f.endswith(".csv"))]
        self.__input_fileNames.sort()


class FetchHistoricalData:
    def __init__(self, api_key: str, freq: int, location: str, start_date: datetime, end_date: datetime,
                 max_retry: int = 5):
        self.__api_key = api_key
        self.__freq = freq
        self.__location_list = [location]
        self.__start_date = start_date.strftime('%d-%b-%Y')
        self.__end_date = end_date.strftime('%d-%b-%Y')
        self.__retry = 1
        self.__max_retry = max_retry

    def fetch(self) -> DataFrame:
        try:
            print("Generating history data for {} from {} to {}, Retry: {}".format(self.__location_list,
                                                                                   self.__start_date, self.__end_date,
                                                                                   self.__retry))
            data = retrieve_hist_data(self.__api_key,
                                      self.__location_list,
                                      self.__start_date,
                                      self.__end_date,
                                      self.__freq,
                                      location_label=False,
                                      export_csv=False,
                                      store_df=True)
            sleep(1)
            return data[0]
            print("Weather history has been Generated")
        except:
            print("Unknown Error while Processing, Processing it again.")
            self.__retry = self.__retry + 1
            if self.__retry <= self.__max_retry:
                self.fetch()


class WeatherData(PreProcessor):
    # Ref https://towardsdatascience.com/obtain-historical-weather-forecast-data-in-csv-format-using-python-5a6c090fc828
    __weather_data_frame: Optional[DataFrame] = None

    def __init__(self, output_path: str, month: int, year: int,
                 api_key: str, optimise_data: bool = True, freq: int = 1):
        super().__init__("", output_path)
        self.__month: int = month
        self.__year: int = year
        self.__freq: int = freq
        self.__api_key_for_wwo = api_key
        self.__optimise_data: bool = optimise_data

    def optimise_data(self):
        if self.__weather_data_frame is not None:
            self.__weather_data_frame['date_time'] = to_datetime(self.__weather_data_frame.date_time)
            self.__weather_data_frame['date'] = self.__weather_data_frame.date_time.dt.date
            self.__weather_data_frame['time_code'] = self.__weather_data_frame['date_time'] \
                .apply(PreProcessor.get_code_for_time)
            self.__weather_data_frame = self.__weather_data_frame[['date', 'time_code', 'totalSnow_cm', 'FeelsLikeC',
                                                                   'precipMM']]

    def fetch_data(self) -> Optional[DataFrame]:
        start_date: datetime = PreProcessor.first_day_of_month(self.__month, self.__year)
        end_date: datetime = PreProcessor.last_day_of_month(start_date)
        history_obj: FetchHistoricalData = FetchHistoricalData(self.__api_key_for_wwo, self.__freq, "New+York",
                                                               start_date, end_date)

        self.__weather_data_frame = history_obj.fetch()
        if self.__optimise_data:
            self.optimise_data()
        return self.__weather_data_frame

    def get_weather_data(self) -> Optional[DataFrame]:
        return self.__weather_data_frame

    def write_as_csv(self):
        if self.__weather_data_frame is not None:
            self.write_dataframe_as_csv(self.__weather_data_frame,
                                        "history_" + str(self.__year) + "_" + str(self.__month))


class YellowTaxiRecord:
    # header: 0 VendorID,
    # 1 tpep_pickup_datetime,
    # 2 tpep_dropoff_datetime,
    # 3 passenger_count,
    # 4 trip_distance,
    # 5 RatecodeID,
    # 6 store_and_fwd_flag,
    # 7 PULocationID,
    # 8 DOLocationID,
    # 9 payment_type,
    # 10 fare_amount,
    # 11 extra,
    # 12 mta_tax,
    # 13 tip_amount,
    # 14 tolls_amount,
    # 15 improvement_surcharge,
    # 16 total_amount
    def __init__(self, raw_rec: List[str], first_day_of_month: datetime, last_day_of_month: datetime,
                 whether_data: DataFrame):
        self.__raw_rec = raw_rec
        # Keep required columns
        self.__pickup_datetime: datetime = datetime.datetime.strptime(raw_rec[1], "%Y-%m-%d%H:%M:%S")
        self.__dropoff_datetime: datetime = datetime.datetime.strptime(raw_rec[2], "%Y-%m-%d%H:%M:%S")
        self.__weekday: int = self.__pickup_datetime.weekday()
        self.__month: int = self.__pickup_datetime.month
        self.__year: int = self.__pickup_datetime.year
        self.__duration: float = ((self.__dropoff_datetime - self.__pickup_datetime).total_seconds()) / 60
        self.__pickup_code: int = PreProcessor.get_code_for_time(self.__pickup_datetime)
        self.__distance: int = float(raw_rec[4])
        self.__totalSnow_cm = 0.0
        self.__feelsLikeC = 0
        self.__precipMM = 0.0
        self.__pulid = raw_rec[7]
        self.__dolid = raw_rec[8]
        self.__first_day_of_month = first_day_of_month
        self.__last_day_of_month = last_day_of_month
        self.__weekday = self.__pickup_datetime.weekday()
        if whether_data is not None and self.is_date_valid() and self.is_trip_duration_is_valid() and self.is_trip_duration_is_valid():
            self.__merge_with_whether_data(whether_data)

    def __merge_with_whether_data(self, whether_data: DataFrame):
        if self.__pickup_code % 2 == 0:
            wwo_row = whether_data[(whether_data['date'] == self.__pickup_datetime.date()) & (
                    self.__pickup_code - 1 == whether_data['time_code'])]
        else:
            wwo_row = whether_data[(whether_data['date'] == self.__pickup_datetime.date()) & (
                    self.__pickup_code == whether_data['time_code'])]
        if wwo_row.empty:
            print("no whether data found for {} with code {}".format(self.__pickup_datetime, self.__pickup_code))
        else:
            self.__totalSnow_cm = wwo_row.iloc[0]['totalSnow_cm']
            self.__feelsLikeC = wwo_row.iloc[0]['FeelsLikeC']
            self.__precipMM = wwo_row.iloc[0]['precipMM']

    def is_date_valid(self) -> bool:
        if self.__pickup_datetime < self.__first_day_of_month or self.__pickup_datetime > self.__last_day_of_month:
            return False
        return True

    def is_trip_duration_is_valid(self) -> bool:
        return self.__duration > 0

    def is_valid_trip_distance(self) -> bool:
        return self.__distance > 0

    @staticmethod
    def out_header() -> List[str]:
        return ["date", "time_code", "pulid", "dolid", "duration", "weekday", "distance", "totalSnow_cm", "FeelsLikeC",
                "precipMM"]

    def get_valid_rec(self) -> Optional[List[str]]:
        if self.is_date_valid() and self.is_trip_duration_is_valid() and self.is_valid_trip_distance():
            return [self.__pickup_datetime.date(), self.__pickup_code, self.__pulid, self.__dolid, self.__duration,
                    self.__weekday, self.__distance, self.__totalSnow_cm, self.__feelsLikeC, self.__precipMM]
        else:
            return None


class YellowTaxiData(PreProcessor):
    __max_rows: int = 10000
    __data: List[List[str]] = []
    __original_file_header: List[str] = []
    __new_header: List[str] = YellowTaxiRecord.out_header()

    def __init__(self, input_file_path: str, output_path: str, month: int, year: int,
                 whether_data: Optional[DataFrame] = None):
        super().__init__(input_file_path, output_path)
        self.__month = month
        self.__year = year
        self.__whether_data = whether_data
        self.__create_empty_csv_file()
        self.__first_day_of_month = PreProcessor.first_day_of_month(self.__month, self.__year)
        self.__last_day_of_month = PreProcessor.last_day_of_month(self.__first_day_of_month)
        if whether_data is not None:
            self.__whether_data['date'] = to_datetime(self.__whether_data.date).dt.date

    @staticmethod
    def get_yellow_file_csv_format(year: int):
        return "yellow_car_" + str(year) + "_{}.csv"

    def __create_empty_csv_file(self):
        csv_file = open(join(self._output_path, "yellow_car_" + str(self.__year) + "_" + str(self.__month) + ".csv"),
                        "w")
        csv.writer(csv_file)
        csv_file.close()

    def __append_records(self, is_header: bool = False):
        with open(join(self._output_path, "yellow_car_" + str(self.__year) + "_" + str(self.__month) + ".csv"),
                  "a") as csv_file:
            csv_writer = csv.writer(csv_file)
            if is_header:
                csv_writer.writerow(self.__new_header)
            else:
                for rec in self.__data:
                    csv_writer.writerow(rec)

    def __is_header_record(self, line_without_space: str) -> bool:
        if line_without_space == "":
            return True
        elif line_without_space.startswith("VendorID,tpep_pickup_datetime"):
            self.__original_file_header = line_without_space.split(",")
            return True
        return False

    def read_and_process_csv(self):
        count: int = 0
        self.__append_records(True)
        with open(self._input_path, 'r') as csv_file:
            for line in csv_file:
                line_without_space = line.strip().replace(" ", "")
                if not self.__is_header_record(line_without_space):
                    count = count + 1
                    rec = YellowTaxiRecord(line_without_space.split(","), self.__first_day_of_month,
                                           self.__last_day_of_month, self.__whether_data).get_valid_rec()
                    if rec is not None:
                        self.__data.append(rec)
                    if count > self.__max_rows:
                        self.__append_records()
                        count = 0
                        self.__data = []
            if count > 0:
                self.__append_records()


class GroupData:
    def __init__(self, file_location: AnyPath, file_name_format: str = "{}.csv"):
        self.__file_location = file_location
        self.__file_name_format = file_name_format
        self.__grouped_data = None

    def count_by_grouping(self) -> DataFrame:
        for month in range(1, 13):
            print("***** Grouping start for month {} *****".format(month))
            input_file = join(self.__file_location, self.__file_name_format.format(month))
            data = read_csv(input_file)
            data["month"] = month
            grouped_month_data = data.groupby(['pulid', 'weekday', 'month', 'time_code', 'totalSnow_cm', 'FeelsLikeC',
                                               'precipMM']).size().to_frame(name='count').reset_index()
            if self.__grouped_data is None:
                self.__grouped_data = grouped_month_data
            else:
                self.__grouped_data.append(grouped_month_data)
            print("***** Grouping start for end {} *****".format(month))
        return self.__grouped_data


def process_weather_data(whether_data_out_path: AnyPath):
    api_key = "1a831061fe684acd873164522203110"
    for year in range(2018, 2020):
        year_data_path: AnyPath = join(whether_data_out_path, str(year))
        PreProcessor.make_dir(year_data_path)
        for month in range(1, 13):
            weather_data = WeatherData(year_data_path, month, year, api_key)
            weather_data.fetch_data()
            weather_data.write_as_csv()


def main():
    location: str = "/Users/archanapatil890/Documents/Machine Learning/python/Capstone/"
    out_dir_path: AnyPath = join(location, "out")
    # PreProcessor.make_dir(out_dir_path)

    whether_data_out_path: AnyPath = join(out_dir_path, "weather_history")
    yellow_car_data_out_path: AnyPath = join(out_dir_path, "yellow_car_out")
    PreProcessor.make_dir(yellow_car_data_out_path)
    # PreProcessor.make_dir(whether_data_out_path)

    # process_weather_data(whether_data_out_path)


if __name__ == "__main__":
    main()
