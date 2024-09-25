import praw
from praw import Reddit as reddit
import sys
from utils.constants import POST_FIELDS
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def connect_reddit(client_id, client_secret, user_agent) -> praw.Reddit:
    try:
        reddit = praw.Reddit(client_id=client_id,
                            client_secret=client_secret,
                            user_agent=user_agent)
        print('connected to reddit!')
        return reddit
    except Exception as e:
        print(f'error in connecting {e}')
        sys.exit(1)

def extract_posts(reddit_instance: praw.Reddit, subreddit: str, time_filter: str, limit=None):
    try:
        subreddit = reddit_instance.subreddit(subreddit)
        logger.info(f'subreddit: {subreddit}')
        posts = subreddit.top(time_filter=time_filter, limit=limit)
        print(posts)
        post_lists = []
        
        for post in posts:
            post_dict = vars(post)
            post = {key: post_dict[key] for key in POST_FIELDS}
            post_lists.append(post)
        return post_lists
    except Exception as e:
        print(f'error in extracting posts {e}')
        sys.exit(1)

if __name__ == '__main__':
    print('in reddit_etl')