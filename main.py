from pipelines import reddit_pipeline
from datetime import datetime

file_postfix = datetime.now().strftime('%Y%m%d')
reddit_pipeline.reddit_pipeline(f'reddit_{file_postfix}','dataengineering',limit=100)