
from package1.acquire import get_main_data
from package1.wrangle import clean_country
from package1.analize import enriching_data
from package1.report import plotting_data


def main():

    final_df = get_main_data()

    cleaned_final_df = clean_country(final_df)

    final_df = getting_data(file_name)
    proc_data = cleaning_data(raw_data)
    table,top = enriching_data(proc_data)
    plotting_data(table,top)

#### no borrar
if __name__ == "__main__":
    main()