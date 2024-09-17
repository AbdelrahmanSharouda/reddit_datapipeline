import praw
import sys

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
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)
    
    posts_lists = []
    
    print(posts)

if __name__ == '__main__':
    print('in reddit_etl')