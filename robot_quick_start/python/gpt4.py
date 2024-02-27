from datetime import datetime
import time
import os, sys
import csv
import bytedtqs
# import euler
import pandas as pd
import re
import openai
# import subprocess
# import asyncio
# import httpx
import numpy as np
# import concurrent

"""TQS Parameters"""
TQS_APP_ID="sKwa3b1yrMTaFriJrmO4EUxVVe1FwRLza0oQaST3nccJfgoO"
TQS_APP_KEY="AK6rBdnwKQ3QIFlQpL0WXgVavidppr1b4GhAL45hom4vYyeC"
TQS_CLUSTER="va"
TQS_PREFIX = """
            set tqs.query.engine.type=Presto; 
"""
TQS_USERNAME="meng.meng"
# TQS_USERNAME="vincent.liu"
TQS_CONF = {'yarn.cluster.name':'mouse','mapreduce.job.queuename':'root.mouse_tiktok_data_us'}
# csv.field_size_limit(2000 * 1024 * 1024) # Needed to increase fetch size
pd.set_option('display.max_columns', None)

def getTQSClient():
    tqsClient = bytedtqs.TQSClient(app_id=TQS_APP_ID,
                                   app_key=TQS_APP_KEY,
                                   cluster=TQS_CLUSTER)
    return tqsClient

def getSQLQuery(date):
    return """
    {prefix}
    SELECT
    text as text,
    id as id,
    link as link,
    CAST(`impression_count` as INT) as impression_count,
    CAST(`like_count` as INT) as like_count,
    CAST(`retweet_count` as INT) as retweet_count,
    company as company,
    country as country
    FROM
        musically_rec.twitter_api_posts 
    WHERE date='{date}' 
    """.format(prefix = TQS_PREFIX, date = date)

def get_event_sql(date):
    return """
    {prefix}
    select * from musically_rec.news_post_events where date='{date}' order by risk_level
    """.format(prefix=TQS_PREFIX, date=date)

def memoize(f):
    cache = {}
    def _call_with_cache(*args, **kwargs):
        key = "{} - {}".format(args, kwargs)
        if key not in cache:
            cache[key] = f(*args, **kwargs)
        return cache[key]
    return _call_with_cache


@memoize
def process_sql(sql_query):
    # print("sql_query = {}".format(sql_query))
    tqs_client = getTQSClient()
    job = tqs_client.execute_query(user_name=TQS_USERNAME, query=sql_query, conf=TQS_CONF)
    if not job.is_success():
        print_log("Failed to load hive table!")
        exit(-1)

    result = job.get_result()
    df = pd.DataFrame(result.fetch_all_data())
    return df


def process_dataframe(result):
    df = pd.DataFrame(result.fetch_all_data())
    df = df.rename(columns=df.iloc[0])
    selected_columns = ['text', 'link', 'id', 'impression_count', 'like_count', 'retweet_count', 'company', 'country']

    df = df[selected_columns]
    df['impression_count'] = pd.to_numeric(df['impression_count'], errors='coerce')
    df['like_count'] = pd.to_numeric(df['like_count'], errors='coerce')
    df['retweet_count'] = pd.to_numeric(df['retweet_count'], errors='coerce')
    df.drop(df.index[0], inplace = True)
    df = df.sort_values("id")


def is_number(string):
    try:
        # Try to convert the string to a float
        float(string)
        return True
    except ValueError:
        # If a ValueError occurs, the string cannot be converted to a number
        return False

def call_openai_chat_completions(client, request_content):
   
    completion = client.chat.completions.create(
        model="gpt-4-0613",
        messages=[
            {
                "role": "system", 
                # "content": "You're a helpful, flexible, smart and professional product manager in a high tech company. You follow instructions extremely well and also help as much as you can. \
                #              You will be given a twitter post and answer users' questions."
                "content": "You're a helpful, flexible, smart and professional product manager in a high tech company. You follow instructions extremely well and also help as much as you can. \
                             You will be given background information and answer users' questions."
            },
            {
                "role": "user",
                "content": request_content
            }
        ],
        temperature=0.00,
        max_tokens=1000

    )

    response = completion.choices[0].message.content
    return response


def print_log(myStr):
    print("\n\n==========\n{myStr}\n==========".format(myStr=myStr))


# async def main(prompts):
#     client = openai.AzureOpenAI(
#         azure_endpoint="https://search-va.byteintl.net/gpt/openapi/offline/v2/crawl",
#         api_version="2023-07-01-preview",
#         api_key="1vteiIwAkTG2Lh7Az7ymhp0AqgcDCHM3"
#     )    
#     results = [] 
#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         tasks = [loop.run_in_executor(executor, call_openai_chat_completions, client, prompt) for prompt in prompts]
#         for response in await asyncio.gather(*tasks):
#             results.append(response.strip("\n"))
    
#     return results

def compose_prompt(date, df, question):
    information = []
    # print(df)
    for ind, row in df[:20].iterrows():
        info = (row['top_topic'],row['news'],row['llm_summary'],row['brand_perception_score'],row['event_source'], row['link'])
        information.append(info)
    prompt = """
    You will be given some events related to the company/app TikTok on {date}, they are detected from news article or generalized from twitter posts.
              Here is the list of tuples: {information} with the format (topic, event_title, event_summary, brand_perception_score, source, link). 
              topic is the aspect of brand perception. 
              event_title is the title that the event is talking about.
              event_summary is the summary that the event is talking about.
              brand_perception_score is the score such that the lower the worse it caused to tiktok's brand perception.
              source is either News or Twitter.
    Based on the given information, your goal is to find out and summarize the most urgent events that worth our attention, and so that we can take actions before the \
              situation escalates. If left unattended, these events will affect TikTok's reputation massively.
              Only answer the major question: {question}
              You will answer the user's question by using definitive words to summarize the most important events that are related to TikTok and specify the date, provide helpful links if necessary, and what's your specific suggestions for TikTok's internal teams at this date if necessary.
    """.format(date=date, information=information, question=question)
    return prompt
    # "Here are some major events detected from news article or generalized from twitter posts. I'll give you one by one "
    # "Could you analyze what's the major problem of TikTok in terms of brand perception this year?"

def gpt_single_reply(prompt):
    client = openai.AzureOpenAI(
        azure_endpoint="https://search-va.byteintl.net/gpt/openapi/offline/v2/crawl",
        api_version="2023-07-01-preview",
        api_key="1vteiIwAkTG2Lh7Az7ymhp0AqgcDCHM3"
    )    
    result = call_openai_chat_completions(client, prompt)
    return result

def get_date(text):
    date_pattern = r'(\d{4}-\d{2}-\d{2})'
    dates = re.findall(date_pattern, text)
    # Flatten the list of tuples into a list of strings, filtering out empty matches
    # dates = [date for group in dates for date in group if date]
    if dates:
        return datetime.strptime(dates[0], '%Y-%m-%d').strftime("%Y%m%d")

    date_pattern = r'(\d{4}/\d{2}/\d{2})'
    dates = re.findall(date_pattern, text)
    if dates:
        return datetime.strptime(dates[0], '%Y/%m/%d').strftime("%Y%m%d")

    date_pattern = r'(\d{8})'
    dates = re.findall(date_pattern, text)
    if dates:
        return dates[0]
    return ""

def entrance(message):
    # date="20231130"
    date = get_date(message)
    if date:
        # sql_query = getSQLQuery(date=date)
        print(f'-- processing date:{date}')
        sql_query = get_event_sql(date=date)
        
        df = process_sql(sql_query)
        df = df.rename(columns=df.iloc[0])
        df['brand_perception_score'] = pd.to_numeric(df['brand_perception_score'], errors='coerce')
        df.drop(df.index[0], inplace = True)
        df = df.sort_values("brand_perception_score", ignore_index=True)
        # print(df)

        prompt = compose_prompt(date, df, message)
        print(prompt)
        # result = gpt_single_reply("Could you analyze what's the major problem of TikTok in terms of brand perception this year?")
        result = gpt_single_reply(prompt)
        print(result)
        return result
    else:
        print('No date detected, processing general question...')
        prompt = message
        result = gpt_single_reply(prompt)
        print(result)
        return result


if __name__ == "__main__":
    # date = get_date('hi 2023/12/12')
    # print(date)
    # message = "how's TikTok's brand perception on 20231128?"
    message = "what's minor protection policy in United States for social media apps?"
    entrance(message)


    # df = process_dataframe(result)    
    # df_size = df.shape[0]
    # print(df)
    # print_log(f"data size: {df_size}")

    # batch_size = 16
    # topics = "Underage/Minor,Regulation,Content Neutrality,Privacy and Data Collection,Content Usefulness,Content Safety,Ad Volume,Ad Content"

    # df['batch'] = np.arange(df_size) // batch_size
    # df['saliencies'] = df_size * ['0.0']
    # df['sentiments'] = df_size * ['0.0']
    # df['topics'] = df_size * [topics]


       
    # df['overall_sentiment'] = df_size * [0.0]
    # df['overall_saliency'] = df_size * [0.0]

    # df['analysis'] = df_size * ['']
    # df['event'] = df_size * ['']
    # df['event_score'] = df_size * [0.0]


   
    # loop = asyncio.get_event_loop()
    # topic_subjects = topics.split(",")

    # for batch, group in df.groupby('batch'):
    #     print(f"Processing batch {batch}")
    #     prompts = group.apply(make_request_content, axis=1).tolist()
    #     resps = loop.run_until_complete(main(prompts))
    #     for i, resp in zip(group.index, resps):
    #         results = resp.split("\n")
    #         print_log(results)
    #         try:
    #             overall_saliency, overall_sentiment, sentiments, saliencies, topics, analysis, event, event_score = parse_results(topic_subjects, results)
    #             print_log(overall_saliency)
    #             print_log(overall_sentiment)
    #             print_log(saliencies)
    #             print_log(sentiments)
    #             print_log(analysis)
    #             print_log(event)

    #         except:
    #             overall_saliency, overall_sentiment, sentiments, saliencies, topics, analysis, event, event_score = 0.0, 0.0, '', '', '', '', '', 0.0
    #             print_log("something wrong happened in parsing.")
    #         print_log(df.loc[i, 'text'])
    #         df.loc[i, 'overall_sentiment'] = overall_sentiment 
    #         df.loc[i, 'overall_saliency'] = overall_saliency
    #         df.loc[i, 'sentiments'] = sentiments
    #         df.loc[i, 'saliencies'] = saliencies
    #         df.loc[i, 'analysis'] = analysis
    #         df.loc[i, 'event'] = event
    #         df.loc[i, 'event_score'] = event_score

    # loop.close()    
        
    # df.drop('batch', axis=1, inplace=True) 

    # hdfs_conf = { 
    #     'hdfs_path': 'hdfs://harunava/default/i18n/recommend/data/warehouse/musically_rec.db/twitter_sentiment_bigben/tmp',
    #     'local_path': './',
    #     'table_name': 'musically_rec.twitter_sentiment_bigben'
    # }
    # write_to_hdfs(tqs_client, df, date, hdfs_conf, date, overwrite=True, offline=False)  
    
