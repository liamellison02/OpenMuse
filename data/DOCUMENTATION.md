# OpenMuse Population Pipeline

## Overview

This system collects NBA data from reliable sources, processes it into natural language documents, generates vector embeddings, and uploads the data to a MongoDB Atlas vector database for use with a RAG-LLM chatbot similar to StatMuse.com.

## System Components

The system consists of the following components:

1. **Data Collector** (`collector.py`): Collects NBA data from the NBA API, including player information, team details, game results, and league statistics.

2. **Data Processor** (`processor.py`): Transforms raw NBA data into natural language documents suitable for RAG applications, following the designed schema.

3. **Embeddings Generator** (`embeddings.py`): Generates vector embeddings for the processed documents using OpenAI's text-embedding-ada-002 model.

4. **MongoDB Connector** (`connector.py`): Connects to MongoDB Atlas and uploads the embedded documents to the vector database.

5. **Database Populator** (`populate.py`): Orchestrates the entire process, providing a unified interface to run the full pipeline or specific parts of it.

## Data Flow

1. Raw NBA data is collected from the NBA API and stored in the `nba_data` directory.
2. The raw data is processed into natural language documents and stored in the `nba_processed_data` directory.
3. Vector embeddings are generated for the processed documents and stored in the `nba_embeddings` directory.
4. The embedded documents are uploaded to the MongoDB Atlas vector database.

## Data Schema

The documents in the MongoDB collection follow this schema:

```json
{
  "_id": ObjectId,
  "text": String,           // The actual text content to be retrieved
  "embedding": [Float],     // Vector embedding of the text (1536 dimensions)
  "category": String,       // Category of the document (player, team, game, stat, etc.)
  "entity_id": String,      // ID of the entity this document refers to
  "season": String,         // NBA season (e.g., "2023-24")
  "metadata": Object,       // Additional metadata for filtering or context
  "created_at": Date,       // Document creation timestamp
  "updated_at": Date        // Document update timestamp
}
```

## Document Categories

The system generates documents in the following categories:

1. **Player Profiles**: Basic information about players, including name, position, height, weight, etc.
2. **Player Statistics**: Career and season statistics for players.
3. **Team Profiles**: Basic information about teams, including name, location, arena, etc.
4. **Team Statistics**: Historical and season statistics for teams.
5. **Game Results**: Summaries of game results.
6. **League Statistics**: League standings and statistical leaders.

## Dependencies

- Python 3.7+
- nba_api
- openai
- pymongo
- python-dotenv
- tqdm
- numpy
- pandas

## Environment Variables

The system requires the following environment variables to be set in a `.env` file:

- `MONGODB_URI`: MongoDB Atlas connection string
- `OPENAI_API_KEY`: OpenAI API key for generating embeddings

## Performance Considerations

- The system uses batch processing to optimize API calls and database operations.
- Caching mechanisms are implemented to avoid duplicate API requests.
- Retry logic is included to handle API rate limits and transient errors.
- The system can be run in different modes to skip already completed steps.

## Error Handling

- Comprehensive error handling and logging are implemented throughout the system.
- Each component logs its operations to a separate log file.
- The main orchestrator provides a unified view of the entire process.

## Extensibility

The system is designed to be easily extended:

- Additional data sources can be added to the data collector.
- New document types can be added to the data processor.
- Different embedding models can be used in the embeddings generator.
- The database connector can be modified to work with different vector databases.
