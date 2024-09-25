import praw
from praw import Reddit as reddit
import sys
from utils.constants import POST_FIELDS,PASSWORD,USERNAME
import logging
import pandas as pd
import numpy as np

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def connect_reddit(client_id, client_secret, user_agent) -> praw.Reddit:
    try:
        reddit = praw.Reddit(client_id=client_id,
                            client_secret=client_secret,
                            user_agent=user_agent,
                            password=PASSWORD,
                            username=USERNAME)
        logger.info(f'connected to reddit!')
        return reddit
    except Exception as e:
        print(f'error in connecting {e}')
        sys.exit(1)

def extract_posts(reddit_instance: praw.Reddit, subreddit: str, time_filter: str, limit=None):
    try:
        subreddit = reddit_instance.subreddit(subreddit)
        # logger.info(f'subreddit: {subreddit}')
        posts = subreddit.top(time_filter=time_filter, limit=limit)
        # logger.info(f'posts: {posts}')
        post_lists = []
        
        for post in posts:
            logger.info(f'post: {post}')
            post_dict = vars(post)
            post = {key: post_dict[key] for key in POST_FIELDS}
            post_lists.append(post)
        logger.info(f'post_lists: {post_lists}')
        return post_lists
    except Exception as e:
        print(f'error in extracting posts {e}')
        sys.exit(1)

def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                 post_df['edited'], edited_mode).astype(bool)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['title'] = post_df['title'].astype(str)

    return post_df

def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)

if __name__ == '__main__':
    print('in reddit_etl')