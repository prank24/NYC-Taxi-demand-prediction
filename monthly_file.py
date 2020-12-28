import sys
from os.path import join

from pandas import read_csv

from preProcessing import YellowTaxiData, AnyPath

if __name__ == "__main__":
    month = int(sys.argv[1:][0])
    location: str = "/Users/archanapatil890/Documents/Machine Learning/python/Capstone/"
    out_dir_path: AnyPath = join(location, "out")
    whether_data_out_path: AnyPath = join(out_dir_path, "weather_history")
    yellow_car_data_out_path: AnyPath = join(out_dir_path, "yellow_car_out")

    out_dir_path: AnyPath = join(location, "out")
    year = 2019
    whether_data_path = join(join(whether_data_out_path, str(year)), "history_" + str(year) + "_" +
                             str(month) + ".csv")
    print("Reading whether file at ", whether_data_path)
    whether_data = read_csv(whether_data_path)
    year_data_path = join(location, str(year))
    monthly_file_path = join(year_data_path, str(month) + ".csv")
    taxi_data = YellowTaxiData(monthly_file_path, yellow_car_data_out_path, month, year, whether_data)
    taxi_data.read_and_process_csv()

    print("prgram is over")
