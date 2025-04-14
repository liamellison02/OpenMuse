# OpenMuse Population Pipeline - Usage Instructions

## Prerequisites

Before using this system, ensure you have:

1. Python 3.7 or higher installed
2. A MongoDB Atlas account with a vector database set up
3. An OpenAI API key with access to embedding models
4. Git installed (to clone the repository)

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd data/scripts/nba
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Create a `.env` file in the root directory with your MongoDB URI and OpenAI API key:

```
MONGODB_URI=your_mongodb_uri_here
OPENAI_API_KEY=your_openai_api_key_here
```

Note: Your MongoDB URI should point to the database where you want to store the NBA data.

### 4. Verify MongoDB Atlas Vector Search Setup

Ensure your MongoDB Atlas cluster has Vector Search enabled and a vector index created on the `nba` collection with the following configuration:

- Index name: `vector_index`
- Field to index: `embedding`
- Dimensions: 1536 (for OpenAI's text-embedding-ada-002 model)
- Similarity metric: cosine

If the index doesn't exist, the script will attempt to create it automatically.

## Running the System

The main script for running the system is `populate.py`. It provides several modes of operation:

### Full Pipeline

To run the complete pipeline (collect, process, embed, and upload):

```bash
python populate.py --mode full
```

This will:
1. Collect NBA data from the NBA API
2. Process the data into natural language documents
3. Generate vector embeddings for the documents
4. Upload the embedded documents to MongoDB Atlas

### Starting from Processed Data

If you already have processed data and want to skip the collection and processing steps:

```bash
python populate.py --mode from_processed
```

### Starting from Embeddings

If you already have embedded data and only want to upload it to MongoDB Atlas:

```bash
python populate.py --mode from_embeddings
```

## Command Line Options

The script supports several command line options:

- `--mode`: Pipeline mode (`full`, `from_processed`, or `from_embeddings`)
- `--player-limit`: Limit the number of players to process (useful for testing)
- `--seasons`: NBA seasons to collect data for (e.g., `--seasons 2022-23 2023-24`)
- `--no-clear`: Don't clear the collection before uploading (append instead)
- `--db-name`: MongoDB database name (default: `OpenMuse`)
- `--collection-name`: MongoDB collection name (default: `nba`)
- `--index-name`: Vector index name (default: `vector_index`)

## Examples

### Testing with a Small Dataset

To test the system with a small dataset (10 players):

```bash
python nba_database_populator.py --mode full --player-limit 10
```

### Processing Multiple Seasons

To collect and process data for multiple NBA seasons:

```bash
python nba_database_populator.py --mode full --seasons 2021-22 2022-23 2023-24
```

### Appending to Existing Data

To add new data without clearing the existing collection:

```bash
python nba_database_populator.py --mode full --no-clear
```

## Running Individual Components

You can also run each component of the system separately:

### Data Collection

```bash
python nba_data_collector.py
```

### Data Processing

```bash
python nba_data_processor.py
```

### Embedding Generation

```bash
python nba_embeddings_generator.py
```

### MongoDB Upload

```bash
python nba_mongodb_connector.py
```

## Monitoring and Logging

Each component of the system logs its operations to a separate log file:

- `nba_data_collector.log`
- `nba_data_processor.log`
- `nba_embeddings_generator.log`
- `nba_mongodb_connector.log`
- `nba_database_populator.log`

You can monitor these logs to track the progress and diagnose any issues.

## Troubleshooting

### API Rate Limits

If you encounter rate limit errors from the NBA API or OpenAI API:

1. Reduce the number of players processed (`--player-limit`)
2. Run the script in stages (`--mode from_processed` or `--mode from_embeddings`)
3. Increase the delay between API calls (modify the scripts)

### MongoDB Connection Issues

If you have trouble connecting to MongoDB Atlas:

1. Verify your MongoDB URI is correct
2. Ensure your IP address is whitelisted in MongoDB Atlas
3. Check that your MongoDB Atlas cluster has Vector Search enabled

### OpenAI API Issues

If you encounter issues with the OpenAI API:

1. Verify your API key is correct and has sufficient quota
2. Check that you have access to the text-embedding-ada-002 model
3. Increase the retry limit and delay in the embeddings generator

## Next Steps

After populating your MongoDB Atlas vector database with NBA data, you can:

1. Use the data with your Next.js RAG-LLM chatbot
2. Periodically update the database with new NBA data
3. Extend the system to collect additional types of NBA data

For more information, refer to the documentation in `documentation.md`.
