import csv
from datetime import datetime


def read_and_convert_csv(input_file_path, output_file_path):
    with open(input_file_path, mode="r") as infile, open(output_file_path, mode="w", newline="") as outfile:
        csv_reader = csv.DictReader(infile, delimiter=",")
        fieldnames = csv_reader.fieldnames + ["timestamp_utc"]
        csv_writer = csv.DictWriter(outfile, fieldnames=fieldnames)

        csv_writer.writeheader()

        CUTOFF = 100
        rows_processed = 0
        for row in csv_reader:
            rows_processed += 1
            if rows_processed > CUTOFF:
                break
            row["timestamp_utc"] = row["TimeUTC"]
            csv_writer.writerow(row)


if __name__ == "__main__":
    input_file_path = "auda_deals.csv"
    output_file_path = "auda_deals_truncated.csv"
    read_and_convert_csv(input_file_path, output_file_path)
