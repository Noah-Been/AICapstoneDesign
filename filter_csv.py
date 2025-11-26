import csv
import os

def filter_csv():
    tickers_file = 'tickers.txt'
    csv_file = 'data_5552_20251126.csv'
    temp_file = 'data_5552_20251126_temp.csv'
    
    # Read tickers
    try:
        with open(tickers_file, 'r', encoding='utf-8') as f:
            tickers = set(line.strip() for line in f if line.strip())
        print(f"Loaded {len(tickers)} tickers from {tickers_file}")
    except FileNotFoundError:
        print(f"Error: {tickers_file} not found.")
        return

    # Read and Filter CSV
    try:
        with open(csv_file, 'r', encoding='utf-8-sig', newline='') as infile, \
             open(temp_file, 'w', encoding='utf-8-sig', newline='') as outfile:
            
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames
            
            if '단축코드' not in fieldnames:
                print("Error: '단축코드' column not found in CSV.")
                os.remove(temp_file)
                return

            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            original_count = 0
            filtered_count = 0

            for row in reader:
                original_count += 1
                if row['단축코드'] in tickers:
                    writer.writerow(row)
                    filtered_count += 1
            
            print(f"Filtered rows: {filtered_count} (removed {original_count - filtered_count} rows)")

        # Overwrite original file
        os.replace(temp_file, csv_file)
        print(f"Overwrote {csv_file} with filtered data.")

    except FileNotFoundError:
        print(f"Error: {csv_file} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
        if os.path.exists(temp_file):
            os.remove(temp_file)

if __name__ == "__main__":
    filter_csv()
