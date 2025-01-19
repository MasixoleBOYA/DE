import os
import pandas as pd

def compare_csv_files(folder_path):
    # Step 1: Read all files in the folder
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    
    if len(csv_files) == 0:
        print("No CSV files found in the folder.")
        return

    print(f"Number of CSV files found: {len(csv_files)}")

    # Load all files into a dictionary
    dataframes = {}
    for file in csv_files:
        try:
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            dataframes[file] = df
        except Exception as e:
            print(f"Error reading {file}: {e}")
            return

    # Step 2: Compare the contents of the files
    # Check for identical files
    first_file, *other_files = csv_files
    base_df = dataframes[first_file]

    all_identical = True

    for other_file in other_files:
        other_df = dataframes[other_file]

        if base_df.equals(other_df):
            print(f"{first_file} and {other_file} are identical.")
        else:
            all_identical = False
            print(f"{first_file} and {other_file} are different.")
            
            # Check for column differences
            if not base_df.columns.equals(other_df.columns):
                base_columns = set(base_df.columns)
                other_columns = set(other_df.columns)
                print(f"Column differences:")
                print(f"  Columns in {first_file} but not in {other_file}: {base_columns - other_columns}")
                print(f"  Columns in {other_file} but not in {first_file}: {other_columns - base_columns}")

            # Check for row differences
            differing_rows = pd.concat([base_df, other_df]).drop_duplicates(keep=False)
            if not differing_rows.empty:
                print(f"Differing rows between {first_file} and {other_file}:")
                differing_rows["Source"] = differing_rows.apply(
                    lambda row: f"{first_file}" if row.name in base_df.index else f"{other_file}", axis=1
                )
                print(differing_rows)

    if all_identical:
        print("All files are identical.")

if __name__ == "__main__":
    folder_path = input("Enter the folder path containing the CSV files: ").strip()
    compare_csv_files(folder_path)
