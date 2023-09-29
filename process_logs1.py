import csv
import pandas as pd
import numpy as np
#import pyshark
import pandas as pd
import advertools as adv
import pandas as pd
from ua_parser import user_agent_parser
import pyarrow.parquet as pq
import pyarrow
from ipywidgets import interact
import ua_parser
pd.options.display.max_columns = None

for p in [adv, pd, pyarrow]:
    print(f'{p.__name__:-<14}v{p.__version__}')

import csv
import os
def process_csv_and_logs(file_path):
    import os

    try:
        processed_data = {}
        # Read the CSV file into a DataFrame
        with open(file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
        logs_df = adv.logs_to_df(
              #logs_df = pd.read_csv(file_path)

            log_file=file_path,
            output_file='output_file1.parquet',
            errors_file='errors_file1.txt',
            log_format='combined'
        )
        


        logs_df = pd.read_parquet('output_file1.parquet')
        logs_df.columns
        data=logs_df
        data['datetime'] = pd.to_datetime(logs_df['datetime'], format='%d/%b/%Y:%H:%M:%S %z', utc=True)
        data['date'] = data['datetime'].dt.date
        data['time'] = data['datetime'].dt.time
        #GROUPLING DATA INTO SETS OF 2 HRS
        def create_time_intervals(timestamp):
            hour = timestamp.hour
            start_hour = (hour // 2) * 2  
            end_hour = start_hour + 2    
            return f"{start_hour:02d}-{end_hour:02d}"
        data['time'] = data['datetime'].apply(create_time_intervals)
        grouped_data = data.groupby('time')
        for interval, group in grouped_data:
            print(f"Time Interval: {interval}")
        unique_time_intervals = data['time'].unique()
        time_interval_arrays = []
        unique_time_intervals.sort()
        for interval in unique_time_intervals:
            filtered_data = data[data['time'] == interval]
            time_interval_arrays.append(filtered_data)
        for group in time_interval_arrays:
            print(f"Time Interval: {group['time'].iloc[0]}")
            print(group)

        # AVG SIZES PLOT
        import matplotlib.pyplot as plt
        def plot_average_sizes(time_interval_arrays):
            time_intervals = []
            avg_sizes = []
            def calculate_avg_size(data_frame):
                avg_size = data_frame['size'].mean()
                return avg_size
            avg_size_result = calculate_avg_size(data)
            print("Average size:", avg_size_result)
            for group in time_interval_arrays:
               interval_avg_size = calculate_avg_size(group)
               time_interval = group['time'].iloc[0]
               time_intervals.append(time_interval)
               avg_sizes.append(interval_avg_size)
            plt.figure(figsize=(10, 6))
            plt.bar(time_intervals, avg_sizes, color='blue')
            plt.xlabel('Time Interval')
            plt.ylabel('Average Size')
            plt.title('Average Size for Each Time Interval')
            plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
            plt.show()

            #TOP 10 SIZES
        def top_ten_sizes_in_dataframe(df):
            top_ten_sizes = df['size'].nlargest(10)
            return top_ten_sizes
        for i, df in enumerate(time_interval_arrays):
                interval = unique_time_intervals[i]
                print(f"Top ten sizes for time interval {interval}:")
                top_sizes = top_ten_sizes_in_dataframe(df)
            
            #STATUS OCCURANCE
        def plot_status_occurrences(df, interval):
                    value_counts = df['status'].value_counts()
                    value_counts_df = value_counts.reset_index()
                    value_counts_df.columns = ['status', 'Count']
                    plt.bar(value_counts_df['status'], value_counts_df['Count'], color=plt.cm.viridis(range(len(value_counts))))
                    plt.xlabel('status')
                    plt.ylabel('Count')
                    plt.title(f'status Occurrences for Time Interval: {interval}')
                    plt.legend(['Status Count'])
                    plt.show()
        for i, df in enumerate(time_interval_arrays):
                    interval = unique_time_intervals[i]
                    plot_status_occurrences(df, interval)
            
        def plot_top_clients(df, interval):
                 top_clients = df['client'].value_counts().head(5)
                 plt.bar(top_clients.index, top_clients.values, color=plt.cm.viridis(range(len(top_clients))))
                 plt.xlabel('Client')
                 plt.xticks(rotation=45)
                 plt.ylabel('Count')
                 plt.title(f'Top 5 Most Occurred Clients for Time Interval: {interval}')
                 plt.legend(['Client Count'])
                 plt.show()
        for i, df in enumerate(time_interval_arrays):
                    interval = unique_time_intervals[i]
                    plot_top_clients(df, interval)
            
        def display_top_requests(df, interval):
                 top_req = df['request'].value_counts().head(5)
                 plt.bar(top_req.index, top_req.values, color=plt.cm.viridis(range(len(top_req))))
                 plt.xlabel('Request')
                 plt.ylabel('Count')
                 plt.title(f'Top 5 Most Occurred Requests for Time Interval: {interval}')
                 plt.xticks(rotation=45)
                 plt.legend(['Request Count'])
                 plt.show()
        for i, df in enumerate(time_interval_arrays):
                 interval = unique_time_intervals[i]
                 display_top_requests(df, interval)

        def plot_top_referers(df, n=5):
                top_ref = df['referer'].value_counts().head(n)
                plt.bar(top_ref.index, top_ref.values, color=plt.cm.viridis(range(len(top_ref))))
                plt.xlabel('Referer')
                plt.ylabel('Count')
                plt.title(f'Top {n} Most Occurred Referers')
                plt.xticks(rotation=45, ha='right')
                plt.legend(['Count'])
                plt.show() 
        for i, df in enumerate(time_interval_arrays):
                interval = unique_time_intervals[i]
                plot_top_referers(df)


        def plot_top_user_agents(df,interval):
                top_agents = df['user_agent'].value_counts().head(5)
                plt.figure(figsize=(11, 6))
                colors = plt.cm.tab20(np.arange(len(top_agents)))
                bars = plt.bar(top_agents.index, top_agents.values, color=colors)
                plt.xticks([])
                legend_labels = []
                for bar, label in zip(bars, top_agents.index):
                    legend_labels.append(bar)
                    bar.set_label(label)

                    plt.legend(handles=legend_labels, title='AGENTS', bbox_to_anchor=(1.05, 1), loc='upper left')
                    plt.ylabel('FREQUENCY')
                    plt.xlabel('AGENTS')
                    plt.title(f'Top 5 User Agents for Time Interval: {interval}')
        plt.show()
        for i, df in enumerate(time_interval_arrays):
               interval = unique_time_intervals[i]
               plot_top_user_agents(df, interval)

        def calculate_max_traffic_in_dataframe(df):
                max_traffic_row = df[df['size'] == df['size'].max()]
                max_traffic_time = max_traffic_row['datetime'].iloc[0]
                max_traffic_value = max_traffic_row['size'].iloc[0]
                return max_traffic_time, max_traffic_value
        for i, df in enumerate(time_interval_arrays):
                interval = unique_time_intervals[i]
                max_traffic_time, max_traffic_value = calculate_max_traffic_in_dataframe(df)
                print(f"Max traffic for time interval {interval}:")
                print(f"Time: {max_traffic_time}")
                print(f"Traffic: {max_traffic_value} size")
                print()
                    
        #print(logs_df.head(10))
       # plot_average_sizes(time_interval_arrays)
        processed_data['logs_df_head'] = logs_df.head(10).to_html()
       #3 processed_data['group'] = group.to_html()
        #processed_data['Average size'] = avg_size_result.to_html()
       # processed_data['Top ten sizes for time interval'] = top_ten_sizes_in_dataframe(df).to_html()
        #processed_data['client'] = df['client'].value_counts().head(5).to_html()
        #processed_data['request'] = df['request'].value_counts().head(5).to_html()
        #processed_data['referer'] = df['referer'].value_counts().head(n).to_html()

       

        

        
       

        #print("Average size:", avg_size_result)

       # print(group)
       # processed_data['plot_average_sizes'] = plot_average_sizes(time_interval_arrays).to_html()
        
        
           
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Get the CSV file path from the user
    return processed_data
