# Description: utils functions for RAG - pinecone & openai
# key functions for usage: 
#  - ask_media(request - question text, dates=None, sources=None, stance=None, model_name = "gpt-3.5-turbo", tokens_out = 512, full_reply = True, top_n = 10 news for summary)
#       - returns one text object:
#               if full_reply = True: request_params + "\n" + "Cost per request: " + str(round(reply_cost,3)) + ". Tokens used: " + str(n_tokens_used) + "\n\n" + reply_text + "\n\n" + news_links
#               if full_reply = False: reply_text
#  - compare_stances(request - question text, summaries_list - retrieved from ask_media, model_name = "gpt-3.5-turbo", tokens_out = 1500)
#      - returns one text object: request_params + "\n\n" + reply_text (if full_reply = False: only reply_text)

# LIBRARIES
import json
import re
import pandas as pd
import time
from datetime import datetime, timedelta
import os
import csv

from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
)  # for exponential backoff

import openai
import pinecone

# CRDENTIALS
# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))
credentials_path = os.path.join(current_dir, 'keys/api_keys.json')
# Load the credentials
with open(credentials_path) as f:
    credentials = json.loads(f.read())  

# load TG credentials
api_id = credentials['api_id'] 
api_hash = credentials['api_hash']
phone = credentials['phone']

#load openai credentials
openai_key = credentials['openai_key']

# load pinecone credentials
pine_key = credentials['pine_key']
pine_env = credentials['pine_env']

# INIT PINECONE
pinecone.init(api_key=pine_key, environment=pine_env)
index_name = 'tg-news'
index = pinecone.Index(index_name)

# INIT OPENAI
openai.api_key = openai_key

#===============================================================================
def clean_text(text):
    # Remove URLs enclosed in brackets
    url_pattern = re.compile(r'\(https?://\S+|www\.\S+\)')
    text = url_pattern.sub('', text)

    # Remove Emojis
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    text = emoji_pattern.sub(r'', text)

    # Remove numbers in brackets at the end
    numbers_pattern = re.compile(r'\(\d+\)\s*$')
    text = numbers_pattern.sub('', text)

    # Remove non-breaking spaces
    text = text.replace('\xa0', ' ')

    #Remove **
    text = text.replace('**', '')

    # Remove double spaces
    text = re.sub(' +', ' ', text)
    # Remove square brackets
    text = text.replace('[', '').replace(']', '')

    return text.strip()  # strip() is used to remove leading/trailing white spaces


#==== RAG (RETRIEVAL AUGMENTATION) FUNCTIONS (based on pinecone & openai) =======
# embed request
def get_embedding(text, model="text-embedding-ada-002"):
   text = text.replace("\n", " ")
   return openai.Embedding.create(input = [text], model=model)['data'][0]['embedding']

# get similar news from PINECONE with filters (dates=None, sources=None, stance=None)
def get_top_pine(request=None, request_emb=None, dates=None, sources=None, stance=None, model="text-embedding-ada-002", top_n=10):
    """
    Returns top news articles related to a given request and stance, within a specified date range.

    Args:
        request (str): The request for which to find related news articles.
        request_emb (numpy.ndarray): The embedding of the request, if already computed.
        dates (list): A list of one or two dates in the format 'YYYY-MM-DD', representing the start and end dates of the date range to search for news articles. If only one date is provided, the end date will be set to today.
        stance (str): The stance of the news articles to search for. Must be one of 'positive', 'negative', or 'neutral'.
        model (str): The name of the OpenAI model to use for computing embeddings.
        top_n (int): The number of top news articles to return.

    Returns:
        Tuple of two strings:
        - The first string contains the summaries of the top news articles.
        - The second string contains the links to the top news articles, along with their similarity scores.
    """
    if request_emb is None and request is None:
        print('Error: no request')
        return
    if request_emb is None:
        request_emb = get_embedding(request)

    dates=dates
    stance=stance[0]
    # define start and end dates (if end date is not defined, it will be set to today)
    if dates:
        if len(dates) == 2:
            # convert start_date to int
            start_date = dates[0]
            end_date = dates[1]
        else:
            start_date = dates[0]
            end_date = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        # set range from 2022-02-01 to today
        start_date = '2000-02-01'
        end_date = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')

    # filtering
    start_date = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
    end_date = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())

    filter = {
        "stance": { "$eq": stance },
        "date": { "$gte": start_date, "$lte": end_date }
        }

    # query pinecone
    res = index.query(request_emb, top_k=top_n, include_metadata=True, filter=filter)
    #save results to txt-file
    with open('pinecone_results.txt', 'w') as f:
        f.write(str(res.to_dict()))
    # check if results are empty
    if res.to_dict()['matches'] == []:
        print('No matches')
        return 'No matches', 'No matches'
    top_sim_news = pd.DataFrame(res.to_dict()['matches']).join(pd.DataFrame(res.to_dict()['matches'])['metadata'].apply(pd.Series))

    # collect links & similarities
    top_sim_news['msg_id'] = top_sim_news['id'].apply(lambda x: x.split('_')[-1])
    top_sim_news['channel_name'] = top_sim_news['id'].apply(lambda x: '_'.join(x.split('_')[:-1]))

    top_sim_news['link'] = top_sim_news.apply(lambda x: "https://t.me/"+str(x.channel_name)+"/"+str(x.msg_id)+" - "+str(round(x.score,3)), axis=1)
    news_links = '\n'.join(top_sim_news['link'].tolist())
    # collect news
    news4request = '\n'.join(top_sim_news['summary'].tolist())
    return news4request, news_links


def get_price_per_1K(model_name):
    if model_name == "gpt-3.5-turbo": #4K (~10 news)
        price_1K = 0.0015 # price per 1000 characters
    if model_name == "gpt-3.5-turbo-1106": #16K (~40 news)
        price_1K = 0.001 # price per 1000 characters
    elif model_name == "gpt-3.5-turbo-16k": #16K (~40 news)
        price_1K = 0.003
    elif model_name == "gpt-4": #8K (~20 news)
        price_1K = 0.03
    elif model_name == "gpt-4-32k": #32K (~80 news)
        price_1K = 0.06
    return price_1K

## Ask OpenAI - returns openau summary based on question and sample of news
@retry(stop=stop_after_attempt(6), wait=wait_random_exponential(multiplier=1, max=10))
def ask_openai(request, news4request, model_name = "gpt-3.5-turbo", tokens_out = 512, language = "ru"):

    system_content_en = f"You are given few short news texts in Russian. Based on these texts you need to answer the following question: {request}. \
        First, analyze if the texts provide an answer to the question. \
        If the texts do not provide proper answer, say that. \
        If they do, select the texts relevant to the question ({request}) and summarize them. \
        \nОтвечай только на русском. Не более 1000 символов."

    system_content_ru = f"Тебе будут представлены несколько новостей. На их основе нужно ответить на вопрос: {request}. \
        Сперрва проверь, что новости содержат ответ. \
        Если ответа в новостях нет, так и ответь. \
        Далее, отбери новости, которые отвечают на вопрос ({request}) и сделай но ним резюме. \
        \nНе более 1000 символов."
    if language == "en": system_content = system_content_en
    elif language == "ru": system_content = system_content_ru
    else: system_content = system_content_ru

    response = openai.ChatCompletion.create(
        model = model_name,
        messages=[
            {
            "role": "system",
            "content": system_content
            },
            {
            "role": "user",
            "content": news4request
            }
        ],
        temperature=0,
        max_tokens=tokens_out,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
        )
    return response

# FUNCTION ask_media to combine all together (TO USE IN TG BOT REQUESTS)
def ask_media(request, dates=None, sources=None, stance=None, model_name = "gpt-3.5-turbo", tokens_out = 512, full_reply = True, top_n = 10):
    # check request time
    request_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    # get top news
    # INPUT: request, dates, sources, stance
    # OUTPUT: news4request - list of news texts for openai, news_links - list of links
    news4request, news_links = get_top_pine(request, dates=dates, sources=sources, stance=stance, model="text-embedding-ada-002", top_n=top_n)
    # limit number of tokens vs model
    if model_name == "gpt-3.5-turbo":
        news4request = news4request[:4000]
    if model_name == "gpt-3.5-turbo-1106":
        news4request = news4request[:8000]
    elif model_name == "gpt-3.5-turbo-16k":
        news4request = news4request[:16000]
    elif model_name == "gpt-4":
        news4request = news4request[:8000]
    elif model_name == "gpt-4-32k":
        news4request = news4request[:32000]
    
    reply = ask_openai(request, news4request, model_name = model_name, tokens_out = tokens_out)
    request_params = f"Request: {request}; \nFilters: dates: {dates}; sources: {sources}; stance: {stance}"
    reply_text = reply.choices[0]['message']['content']
    n_tokens_used = reply.usage.total_tokens
    price_1K = get_price_per_1K(model_name)
    reply_cost = n_tokens_used / 1000 * price_1K

    # write params & reply to file. If file doesn't exist - create it with headers
    # check reply time
    reply_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    
    if not os.path.isfile('openai_chatbot_digest_log.csv'):
        with open('openai_chatbot_digest_log.csv', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['request', 'dates', 'sources', 'stance', 'reply_text', 'reply_cost', 'request_time', 'reply_time', 'model_name', 'n_tokens_used', 'news_links'])
    with open('openai_chatbot_digest_log.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([request, dates, sources, stance, reply_text, reply_cost, request_time, reply_time, model_name, n_tokens_used, news_links])
    
    # return reply for chatbot. If full_reply = False - return only reply_text
    if full_reply == False:
        return reply_text
    else:
        return request_params + "\n" + "Cost per request: " + str(round(reply_cost,3)) + ". Tokens used: " + str(n_tokens_used) + "\n\n" + reply_text + "\n\n" + news_links

# COMPARE STANCES
def compare_stances(request, summaries_list, model_name = "gpt-3.5-turbo", tokens_out = 1500, dates = None, stance = None, sources = None, full_reply = True):
    # check request time
    request_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    system_content_en = f"You are given few texts in Russian from 5 sources on the same subject: {request} \
The structure of the texts is as follows:\
1) name of the source is given in []\
2) the text on the subject above is given.\
You task is to analyse what is similar and what is different in all these texts. First, tell what is similar. Then tell the differences for each source. \
\nОтвечай только на русском. "

    system_content_ru = f"Тебе будут представлены несколько текстов из источников на одну тему: {request} \
Структура текстов следующая:\
1) в [] указан источник\
2) далее идет текст на тему выше.\
Твоя задача - проанализировать, что общего и что разного в этих текстах. Сначала скажи, что общего. \n\
Затем, для каждого источника, скажи, в чем разница в формате: [истчоник] - в чем отличия.\
\nВсего не более 1500 символов."

    reply = openai.ChatCompletion.create(
    model = model_name,
    messages=[
            {
            "role": "system",
            "content": system_content_ru
            },
            {
            "role": "user",
            "content": summaries_list
            }
        ],
        temperature=0,
        max_tokens=tokens_out,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
        )
    request_params = f"Topic: {request}"
    reply_text = reply.choices[0]['message']['content']
    n_tokens_used = reply.usage.total_tokens
    price_1K = get_price_per_1K(model_name)
    reply_cost = n_tokens_used / 1000 * price_1K
    reply_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    if not os.path.isfile('openai_chatbot.csv'):
        with open('openai_chatbot.csv', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['request', 'dates', 'sources', 'stance', 'reply_text', 'reply_cost', 'request_time', 'reply_time', 'model_name', 'n_tokens_used', 'news_links'])
    with open('openai_chatbot.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([request, dates, sources, 'all_summary', reply_text, reply_cost, request_time, reply_time, model_name, n_tokens_used, ""])
    if full_reply == False:
        return reply_text
    return request_params + "\n\n" + reply_text