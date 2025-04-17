# Description: utils functions for RAG - pinecone & openai
# key functions for usage: 
#  - clean_text(text) - returns cleaned text
#  - get_top_pine(request=None, request_emb=None, dates=None, sources=None, stance=None, model="text-embedding-ada-002", top_n=10)
#       - returns two text objects: news4request, news_links
#  - ask_media(request - question text, dates=None, sources=None, stance=None, model_name = "gpt-3.5-turbo", tokens_out = 512, full_reply = True, top_n = 10 news for summary)
#       - uses get_top_pine and ask_openai functions
#       - returns one text object:
#               if full_reply = True: request_params + "\n" + "Cost per request: " + str(round(reply_cost,3)) + ". Tokens used: " + str(n_tokens_used) + "\n\n" + reply_text + "\n\n" + news_links
#               if full_reply = False: reply_text
#  - compare_stances(request - question text, summaries_list - retrieved from ask_media, model_name = "gpt-3.5-turbo", tokens_out = 1500)
#      - returns one text object: request_params + "\n\n" + reply_text (if full_reply = False: only reply_text)

### LIBRARIES
import json
import re
import pandas as pd
import time
import unicodedata
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import sys
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
)
import openai
import cohere
from pinecone import Pinecone
from icecream import ic
import sqlite3

# Load environment variables
load_dotenv()

# Get credentials from environment variables
openai_key = os.getenv('OPENAI_KEY')
cohere_key = os.getenv('COHERE_KEY_PROD')
pine_key = os.getenv('PINE_KEY')
index_name = os.getenv('PINE_INDEX')

# Validate required environment variables
if not all([openai_key, cohere_key, pine_key, index_name]):
    print("Error: Missing required environment variables")
    sys.exit(1)

# INIT PINECONE
# initialize pinecone
pc = Pinecone(api_key=pine_key)
index = pc.Index(index_name)

# INIT OPENAI
openai.api_key = openai_key

# INIT COHERE
co = cohere.Client(cohere_key)

### PARAMETERS
top_n = 30 # number of news to retrieve from pinecone
list_of_stances = ['tv', 'voenkor', 'inet propaganda', 'moder', 'altern'] # possibale stances (as in pinecone DB)

#==============SQL INIT==============================================
# Add after imports
import sqlite3

def init_db():
    conn = sqlite3.connect('news_analysis.db', isolation_level=None)
    conn.execute('PRAGMA encoding = "UTF-8"')
    c = conn.cursor()
    
    # Vector search results
    c.execute('''CREATE TABLE IF NOT EXISTS vector_search
                 (id INTEGER PRIMARY KEY, request_id TEXT, timestamp TEXT,
                  request_text TEXT, stance TEXT, score REAL, news_text TEXT, 
                  news_link TEXT, metadata TEXT)''')
    
    # Reranking results
    c.execute('''CREATE TABLE IF NOT EXISTS reranking
                 (id INTEGER PRIMARY KEY, request_id TEXT, timestamp TEXT,
                  news_id INTEGER, cohere_score REAL, is_relevant INTEGER,
                  FOREIGN KEY(news_id) REFERENCES vector_search(id))''')
    
    # Summaries
    c.execute('''CREATE TABLE IF NOT EXISTS summaries
                 (id INTEGER PRIMARY KEY, request_id TEXT, timestamp TEXT,
                  stance TEXT, summary TEXT, num_news INTEGER, 
                  model TEXT, tokens_used INTEGER, cost REAL)''')
    
    # Comparisons
    c.execute('''CREATE TABLE IF NOT EXISTS comparisons
                 (id INTEGER PRIMARY KEY, request_id TEXT, timestamp TEXT,
                  common_ground TEXT, comparison_json TEXT,
                  model TEXT, tokens_used INTEGER, cost REAL)''')
    
    # New table for full run logging
    c.execute('''CREATE TABLE IF NOT EXISTS full_run
                 (run_id TEXT PRIMARY KEY, timestamp TEXT, data TEXT)''')
    
    conn.commit()
    conn.close()

# Add helper functions for database operations
def generate_request_id():
    return datetime.now().strftime('%Y%m%d_%H%M%S_') + str(hash(str(time.time())))[-4:]

def save_vector_search(request_id, request_text, stance, news_data):
    try:
        conn = sqlite3.connect('news_analysis.db')
        cursor = conn.cursor()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for news in news_data:
            cursor.execute('''INSERT INTO vector_search 
                        (request_id, timestamp, request_text, stance, score, 
                         news_text, news_link, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                     (request_id, timestamp, request_text, stance, 
                      news['score'], news['summary'], news['link'], 
                      json.dumps(news['metadata'])))
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return False
    finally:
        if conn:
            conn.close()

def save_reranking_results(request_id, reranked_data):
    conn = sqlite3.connect('news_analysis.db')
    c = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for item in reranked_data.itertuples():
        c.execute('''INSERT INTO reranking 
                    (request_id, timestamp, news_id, cohere_score, is_relevant)
                    VALUES (?, ?, ?, ?, ?)''',
                 (request_id, timestamp, item.Index, 
                  item.cohere_score, item.is_relevant))
    
    conn.commit()
    conn.close()

def save_summary(request_id, stance, summary, num_news, model, tokens_used, cost):
    conn = sqlite3.connect('news_analysis.db')
    c = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    c.execute('''INSERT INTO summaries 
                (request_id, timestamp, stance, summary, num_news, 
                 model, tokens_used, cost)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
             (request_id, timestamp, stance, summary, num_news, 
              model, tokens_used, cost))
    
    conn.commit()
    conn.close()

def save_comparison(request_id, common_ground, comparison_data, model, tokens_used, cost):
    conn = sqlite3.connect('news_analysis.db')
    c = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    c.execute('''INSERT INTO comparisons 
                (request_id, timestamp, common_ground, comparison_json, 
                 model, tokens_used, cost)
                VALUES (?, ?, ?, ?, ?, ?, ?)''',
             (request_id, timestamp, common_ground, json.dumps(comparison_data), 
              model, tokens_used, cost))
    
    conn.commit()
    conn.close()

def save_full_run(run_data):
    import sqlite3
    conn = sqlite3.connect('news_analysis.db')
    c = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    c.execute('''INSERT INTO full_run (run_id, timestamp, data)
                 VALUES (?, ?, ?)''', (run_data['run_id'], timestamp, json.dumps(run_data)))
    conn.commit()
    conn.close()

#==============CLEAN FUNCTIONS==============================================
# clean topics from TrueStory
def clean_text_topic(text):
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

# standard clean text function
def clean_text(text):
    # Unicode range for emojis
    emoji_pattern = re.compile("["
                               "\U0001F600-\U0001F64F"  # Emoticons
                               "\U0001F300-\U0001F5FF"  # Symbols & Pictographs
                               "\U0001F680-\U0001F6FF"  # Transport & Map Symbols
                               "\U0001F1E0-\U0001F1FF"  # Flags (iOS)
                               "]+", flags=re.UNICODE)

    # Remove emojis
    text = emoji_pattern.sub(r'', str(text))
    # Regular expression for URLs
    url_pattern = re.compile(r"http\S+|www\S+")
    # Remove URLs
    text = url_pattern.sub(r'', str(text))
    # remove /n
    text = text.replace('\n', '. ')
    # Remove any remaining variation selectors
    text = ''.join(char for char in text if unicodedata.category(char) != 'Mn')

    #Remove Foreign Agent text
    pattern_fa = re.compile(r'[А-ЯЁ18+]{3,}\s[А-ЯЁ()]{5,}[^\n]*ИНОСТРАННОГО АГЕНТА')
    text = pattern_fa.sub('', text)
    name1 = 'ПИВОВАРОВА АЛЕКСЕЯ ВЛАДИМИРОВИЧА'
    text = text.replace(name1, '')

    # remove "Subscribe ..."
    pattern_subs = re.compile(r"(Подписаться на|Подписывайтесь на|Подписывайся на).*$", flags=re.MULTILINE)
    text = pattern_subs.sub('', text)

    return text


#==== RAG (RETRIEVAL AUGMENTATION) FUNCTIONS (based on pinecone & openai/cohere) =======
# embed request
@retry(stop=stop_after_attempt(6), wait=wait_random_exponential(multiplier=1, max=10))
def get_embedding(text, model = 'embed-multilingual-v3.0', input_type = 'clustering'):
    response = co.embed(
        texts = text,
        model = model,
        input_type = input_type
                )
    return response.embeddings[0]

# get similar news from PINECONE with filters (dates=None, sources=None, stance=None)
@retry(stop=stop_after_attempt(6), wait=wait_random_exponential(multiplier=1, max=10))
def get_top_pine(request: str=None, request_emb=None, dates: ['%Y-%m-%d',['%Y-%m-%d']]=None, sources=None, stance=None\
                 , model="embed-multilingual-v3.0", top_n=10, join_news=True):
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
        - The first string contains articles.
        - The second string contains the links to the top news articles, along with their similarity scores.
    """
    if request_emb is None and request is None:
        print('Error: no request')
        return
    if request_emb is None:
        request_emb = get_embedding([request])

    dates=dates
    stance=stance[0]
    # check is stance is valid
    if stance not in list_of_stances: return print(f"{stance} is not valid stance. Possible names are: {list_of_stances}")

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

    # transform dates to int (for pinecone filter)
    start_date = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
    end_date = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())

    filter = {
        "stance": { "$eq": stance },
        "date": { "$gte": start_date, "$lte": end_date }
        }

    # query pinecone
    # ic(request_emb, top_n, filter) # check inputs with icecream
    # ic(index.query(request_emb, top_k=top_n, include_metadata=True, filter=filter)) # check pinecone with icecream
    res = index.query(vector=request_emb, top_k=top_n, include_metadata=True, filter=filter)
    # save results to txt-file with forced UTF-8
    with open('pinecone_results.txt', 'w', encoding='utf-8', errors='replace') as f:
        f.write(str(res.to_dict()).encode('utf-8', errors='replace').decode('utf-8'))
    # check if results are empty
    if res.to_dict()['matches'] == []:
        print('No matches')
        # Return empty lists for news, links, and channel names
        return 'No matches', 'No matches', 'No matches'
    top_sim_news = pd.DataFrame(res.to_dict()['matches']).join(pd.DataFrame(res.to_dict()['matches'])['metadata'].apply(pd.Series))

    # collect links & similarities
    top_sim_news['msg_id'] = top_sim_news['id'].apply(lambda x: x.split('_')[-1])
    top_sim_news['channel_name'] = top_sim_news['id'].apply(lambda x: '_'.join(x.split('_')[:-1]))

    # links with similarity scores
    # top_sim_news['link'] = top_sim_news.apply(lambda x: "https://t.me/"+str(x.channel_name)+"/"+str(x.msg_id)+" - "+str(round(x.score,3)), axis=1)
    # links without similarity scores
    top_sim_news['link'] = top_sim_news.apply(lambda x: "https://t.me/"+str(x.channel_name)+"/"+str(x.msg_id), axis=1)

    # collect news & links
    if join_news:
        news4request = '\n'.join(top_sim_news['summary'].tolist())
        news_links = '\n'.join(top_sim_news['link'].tolist())
        channel_names = '\n'.join(top_sim_news['channel_name'].tolist()) # Also join channel names if join_news is True
    else:
        news4request = top_sim_news['summary'].tolist()
        news_links = top_sim_news['link'].tolist()
        channel_names = top_sim_news['channel_name'].tolist() # Keep channel names as a list
    # Return channel names as the third element
    return news4request, news_links, channel_names


def get_price_per_1K(model_name):
    if model_name == "gpt-4o-mini": #4K (~10 news)
        price_1K = 0.0015 # price per 1000 characters
    if model_name == "gpt-3.5-turbo-1106": #16K (~40 news)
        price_1K = 0.0015 # price per 1000 characters
    elif model_name == "gpt-3.5-turbo-16k": #16K (~40 news)
        price_1K = 0.003
    elif model_name == "gpt-4": #8K (~20 news)
        price_1K = 0.03
    elif model_name == "gpt-4-32k": #32K (~80 news)
        price_1K = 0.06
    elif model_name == "gpt-4o": #32K (~80 news)
        price_1K = 0.02
    return price_1K

def cohere_rerank(request: str, sim_news: list, news_links: list, channel_names: list, dates, stance, threshold = 0.8):
    request_id = generate_request_id()
    reranked_docs = co.rerank(model="rerank-multilingual-v2.0", query=request, documents=sim_news)

    # Include channel_names in the DataFrame
    df_reranked = pd.DataFrame({'news': sim_news, 'links': news_links, 'channel_names': channel_names})
    for i in range(len(reranked_docs.results)):
        index = reranked_docs.results[i].index
        df_reranked.loc[index, 'cohere_score'] = reranked_docs.results[i].relevance_score
        df_reranked.loc[index, 'is_relevant'] = 1 if reranked_docs.results[i].relevance_score > threshold else 0

    # Save to database
    # Note: save_reranking_results might need adjustment if you want to store channel names there too.
    save_reranking_results(request_id, df_reranked)

    # Filter relevant news, links, and channel names
    relevant_df = df_reranked[df_reranked['is_relevant']==1]
    news4request = relevant_df['news'].tolist()
    news_links = relevant_df['links'].tolist()
    relevant_channel_names = relevant_df['channel_names'].tolist() # Get relevant channel names
    num_news = len(news4request)
    # Return relevant channel names as well
    return news4request, news_links, relevant_channel_names, num_news, request_id

## Ask OpenAI - returns openai summary based on question and sample of news
@retry(stop=stop_after_attempt(6), wait=wait_random_exponential(multiplier=1, max=10))
def ask_openai(request: str, news4request: list, model_name = "gpt-4o", tokens_out = 512, prompt_language = "ru"):

    system_content_en = f"You are given few short news texts in Russian. Based on these texts you need to answer the following question: {request}. \
        First, analyze if the texts provide an answer to the question. \
        If the texts do not provide proper answer, say that. \
        If they do, select the texts relevant to the question ({request}) and summarize them. \
        \nОтвечай только на русском. Не более 1000 символов."

    system_content_ru = f"Тебе будут представлены несколько новостей. На их основе нужно ответить на вопрос: {request}. \
        Сперва проверь, что новости содержат ответ. \
        Если ответа в новостях нет, так и ответь. \
        Далее, отбери новости, которые отвечают на вопрос ({request}) и сделай по ним резюме. \
        \nНе более 1000 символов."
    
    # prompt for news filtered by cohere
    system_content_ru_fl = f"""
    Тебе будут представлены несколько новостей по теме {request}. \
    Твоя задача - собрать информацию из этих текстов, касающуюся {request}. \
    На основе этой информации сделай краткое описание, которое передаёт суть всех этих текстов, в виде нумерованных пунктов. \
    Описание должно быть короче 500 символов.
    """

    if prompt_language == "en": system_content = system_content_en
    elif prompt_language == "ru": system_content = system_content_ru
    elif prompt_language == "ru_fl": system_content = system_content_ru_fl
    else: system_content = system_content_ru

    if type(news4request) == list:
        news4request = '\n'.join(news4request)

    response = openai.chat.completions.create(
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

# FUNCTION ask_media to combine all together (TO USE IN TG BOT REQUESTS): get top news, filter via cohere, ask openai for summary
def ask_media(request: str, dates: ['%Y-%m-%d',['%Y-%m-%d']] = None, sources = None, stance: [] = None, model_name: str = "gpt-4o", \
              tokens_out: int = 512, full_reply: bool = True, top_n: int = top_n, prompt_language = "ru_fl"):
    request_id = generate_request_id()
    # check request time
    request_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    # get top news
    # INPUT: request, dates, sources, stance
    # OUTPUT: news4request - list of news texts for openai, news_links - list of links, channel_names - list of channel names
    news4request, news_links, channel_names = get_top_pine(request, dates=dates, sources=sources, stance=stance, model="embed-multilingual-v3.0", top_n=top_n, join_news=False)
    # filter news via Cohere ReRank (dates & stance for saving full results to csv)
    if news4request == 'No matches':
        news4request = []
        news_links = [] # Ensure links are also empty
        channel_names = [] # Ensure channel names are also empty
        num_news = 0
    else:
        # Pass channel_names to cohere_rerank and receive filtered channel_names back
        news4request, news_links, channel_names, num_news, request_id = cohere_rerank(request, news4request, news_links=news_links, channel_names=channel_names, dates=dates, stance=stance, threshold = 0.8)

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

    # get params for long reply
    request_params = f"Request: {request}; \nFilters: dates: {dates}; sources: {sources}; stance: {stance}"
    
    if len(news4request) == 0:
        reply_text = 'Нет новостей по теме'   
        n_tokens_used = 0
        reply_cost = 0
    else:
        reply = ask_openai(request, news4request, model_name = model_name, tokens_out = tokens_out, prompt_language = prompt_language)
        reply_text = reply.choices[0].message.content
        n_tokens_used = reply.usage.total_tokens
        price_1K = get_price_per_1K(model_name)
        reply_cost = n_tokens_used / 1000 * price_1K

    # write params & reply to file
    reply_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    
    # Replace CSV logging with database storage
    if len(news4request) > 0:
        save_summary(request_id, stance[0] if stance else None, reply_text, 
                    num_news, model_name, n_tokens_used, 
                    (n_tokens_used / 1000 * get_price_per_1K(model_name)))
    
    # return reply for chatbot. If full_reply = False - return only reply_text
    if full_reply == False:
        ic(stance, reply_text, num_news, news_links, channel_names) # Add channel_names to ic output if needed
        # Return channel_names along with other results
        return reply_text, num_news, news_links, channel_names
    else:
        # Include channel_names in the full reply if desired, or just return them separately if needed elsewhere
        return request_params + "\n" + "Cost per request: " + str(round(reply_cost,3)) + ". Tokens used: " + str(n_tokens_used) + \
            "N of filtered news: "+ str(num_news) + "\n\n" + reply_text + "\n\n" + str(news_links) # Decide how to include channel_names here if needed

# MAKE SUMMARIES for given topic and dates (iterate over stances). Returns 4 dictionaries: summary_dict, num_dict, links_dict, channels_dict.
def make_summaries(topic, dates):
    # collect summaries for each stance
    summary_dict = {}
    num_dict = {}
    links_dict = {}
    channels_dict = {} # Add dictionary for channel names
    for stance in ['tv', 'voenkor', 'inet propaganda', 'moder', 'altern']:
        # Receive channel_names from ask_media
        reply_text, num_news, news_links, channel_names = ask_media(request=topic, dates=dates, stance=[stance], full_reply=False, top_n=20, prompt_language="ru_fl")
        summary_dict[stance] = reply_text
        num_dict[stance] = num_news
        links_dict[stance] = news_links
        channels_dict[stance] = channel_names # Store channel names for the stance
        print(f"Summary for stance {stance} added.")
        time.sleep(0.5)
    # Return channels_dict as well
    return summary_dict, num_dict, links_dict, channels_dict

# COMPARE STANCES
def compare_stances(request, summaries_list, model_name = "gpt-4o", tokens_out = 1500, dates = None, stance = None, sources = None, full_reply = True):
    request_id = generate_request_id()
    # check request time
    request_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    system_content_en = f"You are given few texts in Russian from 5 sources on the same subject: {request} \
The structure of the texts is as follows:\
1) name of the source is given in []\
2) the text on the subject above is given.\
You task is to analyse what is similar and what is different in all these texts. First, tell what is similar. Then tell the differences for each source. \
\nОтвечай только на русском. "

    system_content_ru = f"""Тебе будут представлены несколько текстов из источников на одну тему: {request} \
Структура текстов следующая:\
1) в [] указан источник\
2) далее идет текст на тему выше.\
Твоя задача - проанализировать, что общего и что разного в этих текстах. Сначала скажи, что общего в формате [общее] - что общего. \n\
Затем, для каждого источника, скажи, какая дополнительная информация в неи указана в формате: [истчоник] - дополнительная информация, без вводных фраз и общих оборотов (или "ничего дополнительного").\
\nВсего не более 1500 символов."""

# пример json без указания конкретноых источиков и текстов
    system_content_ru_json = f"""
Тебе будут представлены несколько текстов из источников на одну тему: {request} \
Структура данных тебе текстов следующая:\
1) в [] указан источник\
2) далее идет текст на тему выше.\
Твоя задача - проанализировать, что общего и что разного в этих текстах. 
Сначала напиши, общего. 
Затем, для каждого источника, укажи, какую важную дополнительную информацию он сообщает (или "ничего дополнительного"). 
Выведи ответ в json-формате следующего вида:\
    "общее": "общая информация",
    "источник1": "дополнительная информация из источника 1",
    "источник2": "дополнительная информация из источника 2"
Названия источников нужно указывать строго так, как они указаны в скобках.
Ответ:
"""

    reply = openai.chat.completions.create(
        model = model_name,
        response_format = { "type": "json_object" },
        messages=[
                {
                "role": "system",
                "content": system_content_ru_json
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
    reply_text = reply.choices[0].message.content  # Updated this line
    n_tokens_used = reply.usage.total_tokens
    price_1K = get_price_per_1K(model_name)
    reply_cost = n_tokens_used / 1000 * price_1K
    reply_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    # Replace CSV logging with database storage
    reply_data = json.loads(reply_text)
    save_comparison(request_id, reply_data.get('общее', ''), reply_data,
                   model_name, n_tokens_used, 
                   (n_tokens_used / 1000 * get_price_per_1K(model_name)))
    
    if full_reply == False:
        return reply_text
    return request_params + "\n\n" + reply_text