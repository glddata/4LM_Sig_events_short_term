import io
import os
from blob_connector import AzureBlobStorage
import csv
import configparser
from FourLM_obc_config import get_snooper_headings


class TrainData:
    def __init__(self, vobc_c_header: str):
        self.vobc_c_header = vobc_c_header
        self.data = []
        self.sig_events = []  # initialize signal events as an empty list
        self.fields = get_snooper_headings()

    def add_row(self, row: dict):
        # Initialize any missing fields with blank values
        for field in self.fields:
            if field not in row:
                row[field] = ""
        self.data.append(row)

    def add_target_point_marker(self):
        if len(self.data) > 1:
            # Get the last and second-to-last rows
            last_row = self.data[-1]
            second_last_row = self.data[-2]

            last_target_point = last_row.get("target_point_C_T", "")
            second_last_target_point = second_last_row.get("target_point_C_T", "")

            # Add a default blank value for the Marker_Target_Point_Update column to the last row
            last_row.setdefault("Marker_Target_Point_Update", "")

            if last_target_point != second_last_target_point:
                last_row["Marker_Target_Point_Update"] = last_target_point
                self.sig_events.append(
                    (last_row["date_time_C_Buffer"], "Marker_Target_Point_Update")
                )
            else:
                last_row["Marker_Target_Point_Update"] = ""
        elif len(self.data) == 1:
            # If there is only one row, add a default blank value for the Marker_Target_Point_Update column to the row
            last_row = self.data[-1]
            last_row.setdefault("Marker_Target_Point_Update", "")

    def add_movement_markers(self):
        if len(self.data) < 2:
            return  # skip if there are fewer than 2 rows

        # Get the last and second-to-last rows
        last_row = self.data[-1]
        second_last_row = self.data[-2]

        # Get the actual_velocity_R_T values from the last and second-to-last rows
        last_velocity = last_row.get("actual_velocity_R_T", "")
        second_last_velocity = second_last_row.get("actual_velocity_R_T", "")

        if last_velocity is None or not last_velocity.isdigit():
            last_velocity = second_last_row.get("Marker_Moving", "")
        else:
            last_velocity = int(last_velocity)

        if second_last_velocity is None or not second_last_velocity.isdigit():
            second_last_velocity = last_velocity
        else:
            second_last_velocity = int(second_last_velocity)

        if last_velocity == 0:
            last_row["Marker_Moving"] = "Stationary"
        else:
            last_row["Marker_Moving"] = "Moving"

        if second_last_velocity == 0 and last_velocity != 0:
            last_row["Marker_Arr_Dept"] = "Departed"
        elif second_last_velocity != 0 and last_velocity == 0:
            last_row["Marker_Arr_Dept"] = "Arrived"
        else:
            last_row["Marker_Arr_Dept"] = ""

        # Check if any of the markers are set to true
        if ("Marker_Moving" in last_row and last_row["Marker_Moving"] == "Moving") or (
            "Marker_Arr_Dept" in last_row and last_row["Marker_Arr_Dept"] != ""
        ):
            # Save the row and the event name in self.sig_events
            self.sig_events.append({"event_name": "movement", "row_data": last_row})

        if ("Marker_Target_Point_Update" in last_row) and (
            last_row["Marker_Target_Point_Update"] != ""
        ):
            self.sig_events.append(
                {"event_name": "target_point_update", "row_data": last_row}
            )

    def get_train_data(self):
        # Return a list of dictionaries containing the train data
        return self.data

    def get_vobc_c_header(self):
        return self.vobc_c_header


def process_blob_files(
    connection_string: str, container_name: str, output_file_fol: str
) -> None:
    """
    Process the CSV files in the specified Azure Blob Storage container and export the combined train data to a CSV file.

    Args:
        connection_string (str): The connection string for the Azure Blob Storage account.
        container_name (str): The name of the container to process.
        output_file_path (str): The path of the output CSV file.
    """
    # Create a BlobConnector instance to connect to the Blob Storage account and list the files in the container.
    blob_connector = AzureBlobStorage(connection_string)
    blob_files = blob_connector.list_blobs(container_name)

    # Create a TrainData object for each VOBC_C_Header value
    train_data_dict = {}
    for blob in blob_files:
        # Skip any files that are not CSV files.
        if not blob.endswith(".csv"):
            continue

        if "raw-downloads/Processed_OBC/2023/03/10/" not in blob:
            continue

        print(f"{blob}")

        # if (
        #     blob
        #     != r"raw-downloads/Processed_OBC/2023/03/10/Log_41611_SMA5_20230310_094413.csv"
        # ):
        #     continue

        output_file_path = rf"{output_file_fol}\{os.path.basename(blob)}"
        # Download the file data.
        blob_data = blob_connector.download_blob(container_name, blob)

        # Read the CSV data from a string using a file-like object
        csv_file = io.StringIO(blob_data.decode("utf-8"))
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            # Get the VOBC_C_Header value
            vobc_c_header = row["VOBC_C_Header"]
            if not vobc_c_header:
                continue  # skip this row if the VOBC_C_Header is empty

            # If a TrainData object for this VOBC_C_Header doesn't exist yet, create one
            if vobc_c_header not in train_data_dict:
                train_data_dict[vobc_c_header] = TrainData(vobc_c_header)

            # Add the row data to the TrainData object for this VOBC_C_Header
            train_data_dict[vobc_c_header].add_row(row)
            train_data_dict[vobc_c_header].add_movement_markers()
            train_data_dict[vobc_c_header].add_target_point_marker()

        # Write the data to a CSV file with the marker columns added

        fieldnames = get_snooper_headings()
        with open(output_file_path, "w", newline="") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=fieldnames
                + [
                    "Marker_Moving",
                    "Marker_Arr_Dept",
                    "Marker_Target_Point_Update",
                ],
            )
            writer.writeheader()
            print("write rows of data)")
            for train_data in train_data_dict.values():
                for row in train_data.data:
                    # Combine the contents of the row dictionary with the marker values
                    new_row = row.copy()  # make a copy of the row dictionary
                    new_row.update(
                        {
                            "Marker_Moving": row.get("Marker_Moving", ""),
                            "Marker_Arr_Dept": row.get("Marker_Arr_Dept", ""),
                            "Marker_Target_Point_Update": row.get(
                                "Marker_Target_Point_Update", ""
                            ),
                        }
                    )

                    # Write the combined row to the output CSV file
                    # rowdict = {key: new_row.get(key, "") for key in fieldnames}
                    # for fieldname, value in new_row.items():
                    #     print(fieldname, value)
                    writer.writerow(new_row)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")

    connection_string = config.get("azure_storage", "connection_string")
    container_name = config.get("azure_storage", "container_name")

    process_blob_files(connection_string, container_name, r"out/")


# TODO add VOBC to VOBC check
# TODO add CRC = CRC check
