import pandas as pd
import os


def load_csv_to_df(file_path):
    df = pd.read_csv(file_path)

    return df


def filter_to_ISP(df):
    filtered_df = df[
        (df["Marker_Arr_Dept"] == "Arrived")
        & (df["vcc_R_Buffer"] == 2)
        & (df["rx_loop_R_Header"] == 3)
        & (df["Active_passive_R_T"] == 1)
    ]

    # Display the filtered DataFrame
    print(filtered_df)
    filtered_df.to_clipboard()


def filter_to_BSS2(df):
    # Filter the DataFrame based on the conditions
    filtered_df = df[
        (df["Marker_Arr_Dept"] == "Arrived")
        & (df["vcc_R_Buffer"] == 2)
        & (df["rx_loop_R_Header"] == 5)
        & (df["position_number_R_T"] == 7)
    ]
    # Display the filtered DataFrame
    print(filtered_df)
    filtered_df.to_clipboard()


# if __name__ == "__main__":
#     fpath = r"C:\Users\WORK\OneDrive - CPC Project Services LLP\Shared Documents\Performance\GD_Code\OBC_SIG_EVENTS\out\Log_41611_SMA5_20230310_094413.csv"
#     df = load_csv_to_df(fpath)

#     filter_to_BSS2(df)

if __name__ == "__main__":
    folder_path = r"C:\Users\WORK\OneDrive - CPC Project Services LLP\Shared Documents\Performance\GD_Code\OBC_SIG_EVENTS\out"  # Replace with the folder containing your CSV files
    all_dfs = []

    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(folder_path, file_name)
            df = load_csv_to_df(file_path)
            filtered_df = filter_to_BSS2(df)
            all_dfs.append(filtered_df)

    concatenated_df = pd.concat(all_dfs)
    print(concatenated_df)
