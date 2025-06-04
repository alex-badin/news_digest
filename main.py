# Purpose: get last message from TrueStory channel, summarize it and send to TG digest channel
# Assumption: script runs every hour and should not miss any message from TrueStory
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
app_env = os.getenv('APP_ENV', 'production').lower()

api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
session_string = os.getenv('SESSION_STRING')
min_articles_for_processing_str = os.getenv('MIN_ARTICLES_FOR_PROCESSING', '2') # Default to 2 if not set

channel_id_str = None
channel_id_old_str = None

if app_env == 'development':
    print("INFO: Running in DEVELOPMENT mode.")
    channel_id_str = os.getenv('CHANNEL_ID_TEST')
    channel_id_old_str = os.getenv('CHANNEL_ID_OLD_TEST') # Assuming you might have a separate 'old' test channel
    if not channel_id_old_str: # Fallback if only one test channel ID is defined
        channel_id_old_str = channel_id_str 
        print("INFO: CHANNEL_ID_OLD_TEST not set, using CHANNEL_ID_TEST for both.")
elif app_env == 'production':
    print("INFO: Running in PRODUCTION mode.")
    channel_id_str = os.getenv('CHANNEL_ID_PROD')
    channel_id_old_str = os.getenv('CHANNEL_ID_OLD_PROD')
else:
    print(f"Warning: APP_ENV is set to '{app_env}', which is not recognized. Defaulting to PRODUCTION mode.")
    channel_id_str = os.getenv('CHANNEL_ID_PROD')
    channel_id_old_str = os.getenv('CHANNEL_ID_OLD_PROD')

# Validate required environment variables
missing_vars = []
if not api_id: missing_vars.append('API_ID')
if not api_hash: missing_vars.append('API_HASH')
if not session_string: missing_vars.append('SESSION_STRING')
if not channel_id_str:
    missing_vars.append('CHANNEL_ID_PROD or CHANNEL_ID_TEST (based on APP_ENV)')
if not channel_id_old_str:
    missing_vars.append('CHANNEL_ID_OLD_PROD or CHANNEL_ID_OLD_TEST (based on APP_ENV)')

if missing_vars:
    print("Error: Missing required environment variables:", ", ".join(missing_vars))
    sys.exit(1)

# Convert to integer
api_id = int(api_id)
channel_id = int(channel_id_str)
channel_id_old = int(channel_id_old_str)

try:
    min_articles_for_processing = int(min_articles_for_processing_str)
except ValueError:
    print(f"Error: MIN_ARTICLES_FOR_PROCESSING must be an integer. Found: {min_articles_for_processing_str}")
    sys.exit(1)

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
    await client.send_message(channel_id_old, header)

async def main(client):
    global topics_run_data  # to accumulate run data
    
    for i, topic in enumerate(cleaned_texts):
        print(f"Processing topic #{i}: {topic[:40]}...")
        client.parse_mode = 'html'

        # get summaries, links & channel names for each stance
        summary_dict, num_dict, links_dict, channels_dict = utils.make_summaries(topic, dates)
        tot_num = sum(num_dict.values())  # total number of news

        if tot_num <= min_articles_for_processing:
            print(f"INFO: Skipping topic '{topic[:40]}...' - found only {tot_num} articles (threshold is > {min_articles_for_processing}).")
            topics_run_data.append({
                "topic": topic,
                "status": "skipped",
                "reason": f"Found {tot_num} articles, threshold is > {min_articles_for_processing}.",
                "summaries": { # Store what little was found
                    "summary_dict": summary_dict,
                    "num_dict": num_dict,
                    "links_dict": links_dict,
                    "channels_dict": channels_dict
                },
                "comparison": None,
                "final_post": None
            })
            continue # Skip to the next topic

        # If not skipped, proceed with comparison and posting
        print(f"Sufficient articles ({tot_num}) found for topic '{topic[:40]}...'. Proceeding with comparison.")
        summary_string = '\n'.join([f'[{key}]: {value}' for key, value in summary_dict.items() if num_dict[key] != 0])
        bulk_compare_json = utils.compare_stances(topic, summary_string, dates=dates, full_reply=False)
        bulk_compare_dict = json.loads(bulk_compare_json)
        
        stance_names = {
            'tv': "📺 Российское телевидение",
            'voenkor': "🪖 Военные корреспонденты",
            'inet propaganda': "🏛 Проправительственные источники",
            'moder': "⚖️ Умеренные источники",
            'altern': "🕊 Альтернативные источники"
        }

        post = []
        post.append(f"<b>{bulk_compare_dict['общее']}</b>")
        article_word = "статья" if tot_num % 10 == 1 and tot_num % 100 != 11 else "статьи" if 2 <= tot_num % 10 <= 4 and (tot_num % 100 < 10 or tot_num % 100 >= 20) else "статей"
        post.append(f"→ Мы нашли и проанализировали\n{tot_num} {article_word} по теме.")

        post.append("🔎 <b>КАК РАЗНЫЕ ИСТОЧНИКИ ОСВЕЩАЮТ ЭТО СОБЫТИЕ?</b>")
        for stance, num_news in num_dict.items():
            mapped_stance = stance_names.get(stance, stance)
            if num_news == 0:
                # Оборачиваем "нет новостей по теме" в теги курсива
                post.append(f"<b>{mapped_stance}:</b>\n\n<i>(нет статей)</i>")
                continue
            # Create list of source names with hyperlinks
            # Use channels_dict[stance] to get the channel names
            source_links = [
                f"<a href='{link}'>{channels_dict[stance][i]}</a>"
                # Iterate through links and corresponding channel names for the current stance, limited to 5
                for i, link in enumerate(links_dict[stance][:5])
                # Add a check to prevent index out of bounds if channel names list is shorter (shouldn't happen with current logic but good practice)
                if i < len(channels_dict[stance])
            ]
            source_texts = ", ".join(source_links)
            # Use singular/plural for "статья(и)"
            article_word = "статья" if num_news % 10 == 1 and num_news % 100 != 11 else "статьи" if 2 <= num_news % 10 <= 4 and (num_news % 100 < 10 or num_news % 100 >= 20) else "статей"
            # Оборачиваем строку с количеством статей и источниками в теги курсива
            post.append(f"<b>{mapped_stance}:</b>\n\n{bulk_compare_dict[stance]}\n<i>({num_news} {article_word}: {source_texts})</i>")

        # --- Footer ---
        # Wrap footer in italic tags
        post.append("<i>Написано автоматически с помощью ИИ. Не забывайте проверять факты (например, с @dokopalsya_bot)</i>")

        result = '\n\n'.join(post)

        # Save interim data for this topic into topics_run_data
        # Include channel names in the saved data if needed for logging/debugging
        topics_run_data.append({
            "topic": topic,
            "status": "processed", # Added status for processed items
            "summaries": {
                "summary_dict": summary_dict,
                "num_dict": num_dict,
                "links_dict": links_dict,
                "channels_dict": channels_dict # Add channel names here
            },
            "comparison": bulk_compare_json,
            "final_post": result
        })

        # send compare_reply to both TG channels
        await client.send_message(channel_id, result, link_preview=False)
        await client.send_message(channel_id_old, result, link_preview=False)
        print(f"Sent to both TG channels topic #{i}: {topic[:40]}...")
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


