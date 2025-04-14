#!/usr/bin/env python3
"""
NBA MongoDB Atlas Connector for RAG-LLM Chatbot

This script connects to MongoDB Atlas and provides functionality to
upload embedded NBA data to the vector database.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

import pymongo
from pymongo import MongoClient
from tqdm import tqdm
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nba_mongodb_connector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class NBAMongoDBConnector:
    """
    Connects to MongoDB Atlas and provides functionality to
    upload embedded NBA data to the vector database.
    """
    
    def __init__(self, 
                data_dir: str = "nba_embeddings",
                db_name: str = "OpenMuse",
                collection_name: str = "nba",
                index_name: str = "vector_index"):
        """
        Initialize the NBA MongoDB Atlas connector.
        
        Args:
            data_dir: Directory containing embedded NBA data
            db_name: MongoDB database name
            collection_name: MongoDB collection name
            index_name: Vector index name
        """
        self.data_dir = data_dir
        self.db_name = db_name
        self.collection_name = collection_name
        self.index_name = index_name
        
        # Get MongoDB URI from environment variable
        self.mongodb_uri = os.getenv("MONGODB_URI")
        
        if not self.mongodb_uri:
            logger.error("MONGODB_URI environment variable not set")
            raise ValueError("MONGODB_URI environment variable not set")
        
        # Connect to MongoDB Atlas
        self.client = None
        self.db = None
        self.collection = None
        
        logger.info(f"NBA MongoDB Atlas Connector initialized for database: {db_name}, collection: {collection_name}")
    
    def connect(self) -> bool:
        """
        Connect to MongoDB Atlas.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Connect to MongoDB Atlas
            self.client = MongoClient(self.mongodb_uri)
            
            # Test connection
            self.client.admin.command('ping')
            
            # Get database and collection
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            
            logger.info(f"Connected to MongoDB Atlas: {self.db_name}.{self.collection_name}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to MongoDB Atlas: {e}")
            return False
    
    def disconnect(self) -> None:
        """
        Disconnect from MongoDB Atlas.
        """
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB Atlas")
    
    def check_vector_index(self) -> bool:
        """
        Check if vector index exists.
        
        Returns:
            True if index exists, False otherwise
        """
        try:
            # Get collection indexes
            indexes = self.collection.list_indexes()
            
            # Check if vector index exists
            for index in indexes:
                if index.get("name") == self.index_name:
                    logger.info(f"Vector index '{self.index_name}' exists")
                    return True
            
            logger.warning(f"Vector index '{self.index_name}' does not exist")
            return False
        except Exception as e:
            logger.error(f"Error checking vector index: {e}")
            return False
    
    def create_vector_index(self, dimension: int = 1536) -> bool:
        """
        Create vector index if it doesn't exist.
        
        Args:
            dimension: Dimension of the vector embeddings
            
        Returns:
            True if index created or already exists, False otherwise
        """
        try:
            # Check if index already exists
            if self.check_vector_index():
                return True
            
            # Create vector index
            index_definition = {
                "name": self.index_name,
                "definition": {
                    "mappings": {
                        "dynamic": True,
                        "fields": {
                            "embedding": {
                                "dimensions": dimension,
                                "similarity": "cosine",
                                "type": "knnVector"
                            }
                        }
                    }
                }
            }
            
            # Create index
            self.collection.create_search_index(index_definition)
            
            logger.info(f"Created vector index '{self.index_name}' with dimension {dimension}")
            return True
        except Exception as e:
            logger.error(f"Error creating vector index: {e}")
            return False
    
    def clear_collection(self) -> bool:
        """
        Clear the collection (delete all documents).
        
        Returns:
            True if collection cleared, False otherwise
        """
        try:
            # Delete all documents
            result = self.collection.delete_many({})
            
            logger.info(f"Cleared collection: {result.deleted_count} documents deleted")
            return True
        except Exception as e:
            logger.error(f"Error clearing collection: {e}")
            return False
    
    def upload_documents(self, documents: List[Dict[str, Any]], 
                        batch_size: int = 100,
                        clear_first: bool = False) -> int:
        """
        Upload documents to MongoDB Atlas.
        
        Args:
            documents: List of documents to upload
            batch_size: Number of documents to upload in each batch
            clear_first: Whether to clear the collection before uploading
            
        Returns:
            Number of documents uploaded
        """
        try:
            # Connect to MongoDB Atlas
            if not self.connect():
                return 0
            
            # Clear collection if requested
            if clear_first:
                self.clear_collection()
            
            # Check vector index
            if not self.check_vector_index():
                # Create vector index
                if not self.create_vector_index():
                    logger.error("Failed to create vector index")
                    return 0
            
            # Upload documents in batches
            total_uploaded = 0
            
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i+batch_size]
                
                try:
                    # Insert batch
                    result = self.collection.insert_many(batch)
                    
                    # Count inserted documents
                    inserted_count = len(result.inserted_ids)
                    total_uploaded += inserted_count
                    
                    logger.info(f"Uploaded batch {i//batch_size + 1}/{(len(documents)-1)//batch_size + 1}: {inserted_count} documents")
                except Exception as e:
                    logger.error(f"Error uploading batch {i//batch_size + 1}: {e}")
            
            logger.info(f"Uploaded {total_uploaded} documents to MongoDB Atlas")
            return total_uploaded
        except Exception as e:
            logger.error(f"Error uploading documents: {e}")
            return 0
        finally:
            # Disconnect from MongoDB Atlas
            self.disconnect()
    
    def upload_file(self, filename: str, clear_first: bool = False) -> int:
        """
        Upload documents from a file to MongoDB Atlas.
        
        Args:
            filename: Name of the file containing documents
            clear_first: Whether to clear the collection before uploading
            
        Returns:
            Number of documents uploaded
        """
        try:
            # Load documents
            with open(f"{self.data_dir}/{filename}", "r") as f:
                documents = json.load(f)
            
            if not documents:
                logger.warning(f"No documents found in {filename}")
                return 0
            
            # Upload documents
            return self.upload_documents(documents, clear_first=clear_first)
        except Exception as e:
            logger.error(f"Error uploading file {filename}: {e}")
            return 0
    
    def upload_all_files(self, clear_first: bool = False) -> int:
        """
        Upload documents from all files to MongoDB Atlas.
        
        Args:
            clear_first: Whether to clear the collection before uploading
            
        Returns:
            Number of documents uploaded
        """
        try:
            # Connect to MongoDB Atlas
            if not self.connect():
                return 0
            
            # Clear collection if requested
            if clear_first:
                self.clear_collection()
            
            # Get all JSON files
            json_files = [f for f in os.listdir(self.data_dir) if f.endswith(".json") and f.startswith("embedded_")]
            
            total_uploaded = 0
            
            for filename in tqdm(json_files, desc="Uploading files"):
                # Upload file
                uploaded = self.upload_file(filename, clear_first=False)
                total_uploaded += uploaded
            
            logger.info(f"Uploaded {total_uploaded} documents from {len(json_files)} files")
            return total_uploaded
        except Exception as e:
            logger.error(f"Error uploading all files: {e}")
            return 0
        finally:
            # Disconnect from MongoDB Atlas
            self.disconnect()
    
    def upload_combined_file(self, filename: str = "all_embedded_data.json", 
                           clear_first: bool = True) -> int:
        """
        Upload documents from a combined file to MongoDB Atlas.
        
        Args:
            filename: Name of the combined file
            clear_first: Whether to clear the collection before uploading
            
        Returns:
            Number of documents uploaded
        """
        try:
            # Upload file
            return self.upload_file(filename, clear_first=clear_first)
        except Exception as e:
            logger.error(f"Error uploading combined file {filename}: {e}")
            return 0


if __name__ == "__main__":
    # Create connector
    connector = NBAMongoDBConnector()
    
    # Upload combined file
    connector.upload_combined_file()
