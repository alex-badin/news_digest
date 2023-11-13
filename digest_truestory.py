from telethon import TelegramClient
import json
import re
import pandas as pd
import time
from datetime import datetime, timedelta
import os
import csv
import asyncio

import utils

# CRDENTIALS
current_dir = os.path.dirname(os.path.abspath(__file__))
keys_path = current_dir+'/keys/'
with open(keys_path+'api_keys.json') as f:
  credentials = json.loads(f.read())
# load TG credentials
api_id = credentials['api_id'] 
api_hash = credentials['api_hash']
phone = credentials['phone']

# TELEGRAM CLIENT SESSION
session_path = 'session/'
if not os.path.exists(session_path):
    os.makedirs(session_path)

# csv file tracking messages id
truestory_ids = 'truestory_ids.csv'
if not os.path.exists(truestory_ids):
    with open(truestory_ids, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['id', 'date'])

## LOAD NEWS DIGEST from TrueStory: get last message (assumption - script runs every hour and should not miss any message)
async def get_last_message(api_id, api_hash, truestory_ids):
    async with TelegramClient(session_path+'session_digest', api_id, api_hash) as client:
        messages = await client.get_messages('truesummary', limit=1)
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


## RAG, COMPARE & SEND to TG channel
# dates for filtering
dates = []
dates.append((datetime.today() - timedelta(days=days_offset)).strftime('%Y-%m-%d'))
dates.append((datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d'))

# Apply the function to your list
cleaned_texts = [utils.clean_text(text) for text in text_list]
header_text = utils.clean_text(header_text)
if days_offset == 7: header_text = header_text + f" ({dates[0]} - {dates[1]})" # add dates to header if weekly digest
head_len = len(header_text)
header = f"{'='*head_len}\n{header_text}\n{'='*head_len}"

# params for summary of each stance
n_tokens_out = 512
full_reply = False

model_name = "gpt-3.5-turbo"
price_1K = utils.get_price_per_1K(model_name)

# send header to TG channel
header_text = header_text + f" ({dates[0]} - {dates[1]})"
head_len = len(header_text)
header = f"{'='*head_len}\n{header_text}\n{'='*head_len}"
async def send_header():
    async with TelegramClient(session_path+'session_digest', api_id, api_hash) as client:
        await client.send_message(-1002138728748, header)

async def main():
    await send_header()
    for i, topic in enumerate(cleaned_texts):
        print(f"Starting opeani summary of topic #{i}: {cleaned_texts[i][:40]}")
        # get summaries for all stances
        summary_list = []
        for stance in ['tv', 'voenkor', 'inet propaganda', 'moder', 'altern']:
            reply_text = utils.ask_media(topic, dates=dates, stance=[stance], model_name = model_name, tokens_out = n_tokens_out, full_reply = full_reply)
            summary_list.append(str([stance])+ "\n" + reply_text)
            # status update
            print(f"Summary for stance {stance} added.")

        summary_list = '\n\n'.join(summary_list)
        # compare summaries (text w/o params)
        compare_reply = utils.compare_stances(topic, summary_list, model_name = model_name, dates=dates, full_reply = False)
        print("Finished opeani summary of stances")
        print(compare_reply)

        # combine reply
        result = f"Тема: {topic}: \n\n{compare_reply}"

        # send compare_reply to TG channel
        async with TelegramClient(session_path+'session_digest', api_id, api_hash) as client:
            await client.send_message(-1002138728748, result)
            print(f"Sent to TG channel topic {i}")
        time.sleep(1)

asyncio.run(main())

# add message id to csv file to not process it again
with open(truestory_ids, 'a', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow([message_id, date])


