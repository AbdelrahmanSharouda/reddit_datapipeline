from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH
from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv
import pandas as pd


def reddit_pipeline(file_name:str, subreddit:str, time_filter='day', limit=None):
    # TODO:
    # connecting to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')
    # extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    # transformation
    post_df = transform_data(post_df)

    # loading to CSV
    load_data_to_csv(post_df, f'{OUTPUT_PATH}/{file_name}.csv')

    return  f'{OUTPUT_PATH}/{file_name}.csv'

if __name__ == '__main__':
    print(CLIENT_ID)
