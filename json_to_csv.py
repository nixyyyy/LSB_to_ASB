import json
import csv
import argparse

# Load data from the NDJSON file
def load_from_ndjson(filename):
    data = []
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            item = json.loads(line.strip())
            data.append(item)
    return data

def main(input_file, output_file):
    data = load_from_ndjson(input_file)

    # Convert the loaded data to CSV format
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Merged At", "Title", "PR URL", "Commit SHA", "Commit Message", "Commit URL"])  # headers
        for item in data:
            for commit in item["commits"]:
                writer.writerow([item["merged_at"], item["title"], item["url"], commit["sha"], commit["message"], commit["url"]])

if __name__ == '__main__':
    # Set up the argument parser
    parser = argparse.ArgumentParser(description='Convert NDJSON to CSV.')
    parser.add_argument('input_file', type=str, help='The NDJSON input file.')
    parser.add_argument('output_file', type=str, help='The CSV output file.')

    # Parse the command line arguments
    args = parser.parse_args()

    # Call the main function with the parsed arguments
    main(args.input_file, args.output_file)
