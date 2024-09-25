from utils.constants import CLIENT_ID, SECRET
from etls.reddit_etl import connect_reddit, extract_posts

def reddit_pipeline(file_name:str, subreddit:str, time_filter='day', limit=None):
    # TODO:
    # connecting to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')
    # extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    # transformation
    # loading to CSV

if __name__ == '__main__':
    print(CLIENT_ID)