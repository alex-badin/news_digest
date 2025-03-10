# Purpose: get last message from TrueStory channel, summarize it and send to TG channel
# Assumption: script runs every hour and should not miss any message

from telethon import TelegramClient
from telethon.sessions import StringSession
import json
import time
from datetime import datetime, timedelta
import os
import sys
from dotenv import load_dotenv
import csv
import asyncio
from icecream import ic

import utils
from utils import init_db, generate_request_id, save_full_run

# Load environment variables
load_dotenv()

# Get credentials from environment variables
api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
channel_id = os.getenv('CHANNEL_ID')
session_string = os.getenv('SESSION_STRING')

# Validate required environment variables
if not all([api_id, api_hash, channel_id, session_string]):
    print("Error: Missing required environment variables")
    sys.exit(1)

# Convert api_id and channel_id to integer as they come as strings from env
api_id = int(api_id)
channel_id = int(channel_id)

# csv file tracking messages id
truestory_ids = 'truestory_ids.csv'
if not os.path.exists(truestory_ids):
    with open(truestory_ids, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['id', 'date'])

# Initialize database
try:
    init_db()
except Exception as e:
    print(f"Failed to initialize database: {e}")
    sys.exit(1)

# Reconfigure stdout to use UTF-8
sys.stdout.reconfigure(encoding='utf-8')

## LOAD NEWS DIGEST from TrueStory: get last message (assumption - script runs every hour and should not miss any message)
async def get_last_message(api_id, api_hash, truestory_ids):
    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    async with client:
        channel = await client.get_entity('truesummary')
        messages = await client.get_messages(channel, limit=1)
        # messages = await client.get_messages('truesummary', limit=1)
        message = messages[0]
    # get key params from message
    text = message.text
    date = message.date
    date = date.strftime("%Y-%m-%d")
    message_id = message.id
    # check if it was already processed. If yes, return None
    with open(truestory_ids, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if str(message_id) in row:
                print(f"Message {message_id} was already processed.")
                return None, None, None, None, None
    # parse the message into header and headlines
    text_list = text.split('\n\n')
    header_text = text_list[0]
    # check type of digest
    if not "Самое важное" in header_text:
        print(header_text)
        # add message id to csv file to not process it again
        with open(truestory_ids, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([message_id, date])
        return None, None, None, None, None
    elif "Самое важное на" in header_text: days_offset = 1
    elif "Самое важное за неделю" in header_text: days_offset = 7
    return header_text, text_list[1:-1], days_offset, date, message_id


header_text, text_list, days_offset, date, message_id = asyncio.run(get_last_message(api_id, api_hash, truestory_ids))

if header_text is None:
    print("No new messages. Exiting.")
    sys.exit()

# After TrueStory message is processed, generate a run_id and store true_story data for the run
run_id = generate_request_id()
true_story_data = {
    "header": header_text,
    "text_list": text_list,
    "message_id": message_id,
    "date": date,
    "days_offset": days_offset
}
# Initialize a list to accumulate topic processing data
topics_run_data = []

# rest of the script

## RAG, COMPARE & SEND to TG channel
# dates for pinecone filtering
dates = []
dates.append((datetime.today() - timedelta(days=days_offset)).strftime('%Y-%m-%d'))
dates.append((datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d'))

# prepare topics and header
cleaned_texts = [utils.clean_text_topic(text) for text in text_list]
header_text = utils.clean_text_topic(header_text)
if days_offset == 7: header_text = header_text + f" ({dates})" # add dates to header if weekly digest
else: header_text = header_text + f" ({date})"
head_len = min(len(header_text), 30) # min=30 due to smartphone screen width
header = f"{'='*head_len}\n{header_text}\n{'='*head_len}"

ic(header_text, cleaned_texts)

# params for summary of each stance
full_reply = False

async def send_header(client):
    await client.send_message(channel_id, header)

async def main(client):
    global topics_run_data  # to accumulate run data
    for i, topic in enumerate(cleaned_texts):
        print(f"Starting OpenAI summary of topic #{i}: {topic[:40]}")
        client.parse_mode = 'html'

        # get summaries & links for each stance
        summary_dict, num_dict, links_dict = utils.make_summaries(topic, dates)
        tot_num = sum(num_dict.values())  # total number of news
        # compare stances
        summary_string = '\n'.join([f'[{key}]: {value}' for key, value in summary_dict.items() if num_dict[key] != 0])
        bulk_compare_json = utils.compare_stances(topic, summary_string, dates=dates, full_reply=False)
        bulk_compare_dict = json.loads(bulk_compare_json)
        # assemble TG post:
        stance_names = {
            'tv': "📺 Российское телевидение",
            'voenkor': "🪖 Военные корреспонденты",
            'inet propaganda': "🏛 Проправительственные источники",
            'moder': "⚖️ Умеренные источники",
            'altern': "🕊 Альтернативные источники"
        }
        
        post = []
        # Change header from "Общее" to "Новость:"
        post.append(f"<b>Новость:</b> {bulk_compare_dict['общее']} (кол-во новостей: {tot_num})")
        post.append("🔎 Как разные источники освещают это событие?")
        for stance, num_news in num_dict.items():
            mapped_stance = stance_names.get(stance, stance)
            if num_news == 0:
                post.append(f"{mapped_stance}: нет новостей по теме")
                continue
            # Create list of "Источник X" with hyperlinks
            source_links = [
                f"<a href='{link}'>Источник {i+1}</a>" 
                for i, link in enumerate(links_dict[stance][:5])
            ]
            source_texts = ", ".join(source_links)
            # Use singular/plural for "статья(и)"
            article_word = "статья" if num_news == 1 else "статьи"
            post.append(f"{mapped_stance}: {bulk_compare_dict[stance]}\n({num_news} {article_word}: {source_texts})")
        result = '\n\n'.join(post)

        # Save interim data for this topic into topics_run_data
        topics_run_data.append({
            "topic": topic,
            "summaries": {
                "summary_dict": summary_dict,
                "num_dict": num_dict,
                "links_dict": links_dict
            },
            "comparison": bulk_compare_json,
            "final_post": result
        })

        # send compare_reply to TG channel
        await client.send_message(channel_id, result, link_preview=False)
        print(f"Sent to TG channel topic {i}")
        time.sleep(1)

async def run():
    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    async with client: 
        await send_header(client)
        await main(client)

asyncio.run(run())

# Store full run information to database
full_run_data = {
    "run_id": run_id,
    "true_story": true_story_data,
    "topic_data": topics_run_data
}
save_full_run(full_run_data)

# add message id to csv file to not process it again
with open(truestory_ids, 'a', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow([message_id, date])


