import json
import pandas as pd
import os
import glob
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files

def process_course_file( engine, filepath):

    df = pd.read_json(filepath, orient= 'records')

    # insert data  to course table (Fact Table)

    course_data = df[['course_id', 'author', 'published_timestamp', 'course_title', 'subject']]
    print(course_data)
    course_data.to_sql('course', if_exists='append', con=engine, index=False)
    print( "course (flat table) ")

    # insert course data  to course_details dimension table

    course_details = df[['course_id','level', 'content_duration','num_subscribers', 'num_reviews', 'num_lectures']]
    course_details.to_sql('course_details', if_exists='append', con=engine, index=False)
    print("course_details(dimension table)")

    # insert price to price_details dimension table

    price_details = df[['course_id', 'price', 'is_paid']]
    price_details.to_sql('price_details', if_exists='append', con=engine, index=False)
    print("price_details(dimension table)")

    # insert time to time_details dimension table

    t = pd.to_datetime(df['published_timestamp'], unit='ns')
    print(t)

    time_data = (t, t.dt.year, t.dt.month, t.dt.day, t.dt.hour, t.dt.minute, t.dt.second)
    column_labels = ('published_time', 'year', 'month', 'day', 'hour', 'minute', 'second')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    print("x",time_df)

    time_df.to_sql('time_details', if_exists='append', con=engine, index=False)
    print(("time_details(dimension table)"))

    # insert data to author_details dimension table

    author_data = print(df.author.apply(lambda x: pd.Series(str(x).split("_"))))
    df[['first_name','last_name','author_code']] = df.author.apply(lambda x: pd.Series(str(x).split("_")))
    print(df)
    author_data = df[['author', 'first_name', 'last_name', 'author_code']]
    print(author_data)

    author_data.to_sql('author_details', if_exists='append', con=engine, index=False)
    print(("author_details(dimension table)"))


def process_data(engine, filepath, func):

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(engine, datafile)
        print('{}/{} files processed.'.format(i, num_files))


def main():
    engine = create_engine(URL(user="xxxxx",
                               password="xxxxx",
                               account="kb90910.europe-west2.gcp",
                               warehouse="COURSEDATA_WH",
                               database="WTRAINER",
                               schema="PUBLIC",
                               ))

    print('Snowflake DB Connection Successful')
    connection = engine.connect()
    connection.close()
    engine.dispose()

    process_data(engine, filepath='data/course_data', func=process_course_file)




if __name__ == "__main__":
    main()