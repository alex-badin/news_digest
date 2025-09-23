# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Russian news analysis project that automatically compares how different types of Russian media cover key events. The system:

1. Downloads news digests from the True Story Summary Telegram channel
2. For each headline, retrieves summaries from different Russian media stance categories
3. Compares the summaries using AI models
4. Publishes comparison results to Telegram channels

## Core Architecture

### Main Components

- **main.py**: Entry point that orchestrates the entire pipeline - fetches Telegram messages, processes topics, generates comparisons, and publishes results
- **utils.py**: Contains all core functionality including RAG operations, AI model interactions, text processing, and database operations

### Data Flow

1. **Message Retrieval**: Uses Telethon to fetch latest digest from True Story channel
2. **Topic Processing**: Extracts and cleans topics from the digest
3. **Vector Search**: Uses Pinecone to find relevant news articles by stance and date
4. **Reranking**: Cohere reranks results for relevance
5. **Summarization**: OpenAI generates summaries per stance
6. **Comparison**: OpenAI compares different stance summaries
7. **Publishing**: Sends formatted results to Telegram channels

### Media Stance Categories

The system categorizes Russian media into 5 stances:
- `tv`: Russian television
- `voenkor`: Military correspondents
- `inet propaganda`: Pro-government sources
- `moder`: Moderate sources
- `altern`: Alternative sources

## Environment Configuration

The project uses environment variables for configuration. Key variables:

- `APP_ENV`: Set to "development" or "production" to control which Telegram channels are used
- `API_ID`, `API_HASH`, `SESSION_STRING`: Telegram API credentials
- `OPENAI_KEY`: OpenAI API key
- `COHERE_KEY_PROD`: Cohere API key for reranking
- `PINE_KEY`, `PINE_INDEX`: Pinecone vector database credentials
- `MIN_ARTICLES_FOR_PROCESSING`: Minimum articles threshold (default: 2)

## Common Commands

### Running the Application
```bash
# Activate virtual environment (if using one)
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate.bat  # On Windows

# Install dependencies
pip install -r requirements.txt

# Run the main script
python main.py
```

### Windows Batch Script
The project includes `run_script_digest.bat` for Windows automation that activates the virtual environment and runs the script.

## Database Schema

The project uses SQLite (`news_analysis.db`) with these main tables:
- `vector_search`: Stores Pinecone search results
- `reranking`: Stores Cohere reranking results
- `summaries`: Stores OpenAI-generated summaries per stance
- `comparisons`: Stores stance comparison results
- `full_run`: Stores complete run data for analysis

## Key Functions in utils.py

- `make_summaries(topic, dates)`: Generates summaries for all stances for a given topic
- `compare_stances(request, summaries_list)`: Compares summaries across stances
- `get_top_pine()`: Retrieves relevant news from Pinecone vector database
- `cohere_rerank()`: Filters search results for relevance
- `ask_openai()`: Generates summaries using OpenAI models

## Processing Logic

The system processes topics with a minimum article threshold. Topics with insufficient articles (≤ MIN_ARTICLES_FOR_PROCESSING) are skipped. For processed topics, it:

1. Retrieves news articles for each stance
2. Filters articles using Cohere reranking (threshold: 0.8)
3. Generates stance-specific summaries
4. Creates cross-stance comparisons
5. Formats results with source links and article counts
6. Publishes to both current and archive Telegram channels

## Dependencies

Key external services:
- **Telegram API**: For message retrieval and publishing
- **Pinecone**: Vector database for news article search
- **Cohere**: For relevance reranking of search results
- **OpenAI**: For text summarization and comparison generation

Main Python packages: telethon, openai, cohere, pinecone, pandas, sqlite3