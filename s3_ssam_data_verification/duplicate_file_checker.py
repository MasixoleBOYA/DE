import os
from collections import defaultdict

def find_duplicate_filenames(parent_folder):
    # Dictionary to store file names and the list of folders they appear in
    file_locations = defaultdict(list)

    # Walk through the folder structure
    for root, dirs, files in os.walk(parent_folder):
        for file in files:
            if file.endswith('.csv'):
                relative_folder = os.path.relpath(root, parent_folder)
                file_locations[file].append(relative_folder)

    # Identify duplicates and record their locations
    duplicates = {file: locations for file, locations in file_locations.items() if len(locations) > 1}

    if not duplicates:
        print("No duplicate file names found.")
    else:
        print("Duplicate file names and their locations:")
        for file, locations in duplicates.items():
            print(f"{file} appears in:")
            for location in locations:
                print(f"  - {location}")

        # Save the results to a file
        output_file = os.path.join(parent_folder, "duplicate_file_report.txt")
        with open(output_file, "w") as f:
            f.write("Duplicate file names and their locations:\n")
            for file, locations in duplicates.items():
                f.write(f"{file} appears in:\n")
                for location in locations:
                    f.write(f"  - {location}\n")
        print(f"Report saved to {output_file}")

if __name__ == "__main__":
    parent_folder = input("Enter the parent folder path: ").strip()
    find_duplicate_filenames(parent_folder)
