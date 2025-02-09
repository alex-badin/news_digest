# Purpose: get last message from TrueStory channel, summarize it and send to TG channel
# Assumption: script runs every hour and should not miss any message

from telethon import TelegramClient
from telethon.sessions import StringSession
import json
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import csv
import asyncio
from icecream import ic

import utils
import sys
from utils import init_db

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
    for i, topic in enumerate(cleaned_texts):
        print(f"Starting opeani summary of topic #{i}: {cleaned_texts[i][:40]}")
        client.parse_mode = 'html'

        # get summaries & links for each stance
        summary_dict, num_dict, links_dict = utils.make_summaries(topic, dates)
        tot_num = sum(num_dict.values()) # total number of news
        # compare stances
        summary_string = '\n'.join([f'[{key}]: {value}' for key, value in summary_dict.items() if num_dict[key] != 0]) # string for openai comparison (only non-empty stances)
        bulk_compare_json = utils.compare_stances(topic, summary_string, dates=dates, full_reply=False)
        bulk_compare_dict = json.loads(bulk_compare_json) # convert to dict
        # assemble TG post:
        post = []
        # common ground
        post.append(f"<b><u>Общее</u></b> (кол-во новостей: {tot_num}): {bulk_compare_dict['общее']}")
        # differences
        for stance, num_news in num_dict.items():
            if num_news == 0:
                post.append(f"<b><u>{stance}</u></b>: нет новостей по теме")
                continue
            links = ", ".join([f"<a href='{link}'>{str(i+1)}</a>" for i, link in enumerate(links_dict[stance][:5])])
            post.append(f"<b><u>{stance}</u></b> ({num_news}, ссылки: {links}): {bulk_compare_dict[stance]}")

        result = '\n\n'.join(post)

        # send compare_reply to TG channel
        await client.send_message(channel_id, result, link_preview=False)
        print(f"Sent to TG channel topic {i}")
        # Cohere trial key is limited to 10 API calls / minute => sleep for 30 sec per 5 calls (stances).
        time.sleep(30)

async def run():
    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    async with client: 
        await send_header(client)
        await main(client)

asyncio.run(run())

# add message id to csv file to not process it again
with open(truestory_ids, 'a', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow([message_id, date])


