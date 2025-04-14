#!/usr/bin/env python3
"""
NBA Vector Embeddings Generator for RAG-LLM Chatbot

This script generates vector embeddings for processed NBA data using
OpenAI's text-embedding-ada-002 model and prepares them for insertion
into a MongoDB Atlas vector database.
"""

import os
import json
import time
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

import numpy as np
from tqdm import tqdm
from dotenv import load_dotenv
from openai import OpenAI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nba_embeddings_generator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class NBAEmbeddingsGenerator:
    """
    Generates vector embeddings for processed NBA data using
    OpenAI's text-embedding-ada-002 model.
    """
    
    def __init__(self, 
                data_dir: str = "nba_processed_data", 
                output_dir: str = "nba_embeddings",
                embedding_model: str = "text-embedding-ada-002"):
        """
        Initialize the NBA embeddings generator.
        
        Args:
            data_dir: Directory containing processed NBA data
            output_dir: Directory to save embeddings
            embedding_model: OpenAI embedding model to use
        """
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.embedding_model = embedding_model
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize OpenAI client
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        if not os.getenv("OPENAI_API_KEY"):
            logger.error("OPENAI_API_KEY environment variable not set")
            raise ValueError("OPENAI_API_KEY environment variable not set")
        
        logger.info(f"NBA Embeddings Generator initialized with model: {embedding_model}")
    
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate embedding for a single text using OpenAI API.
        
        Args:
            text: Text to generate embedding for
            
        Returns:
            Vector embedding as a list of floats
        """
        try:
            response = self.client.embeddings.create(
                model=self.embedding_model,
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            raise
    
    def generate_embeddings_batch(self, texts: List[str], 
                                 batch_size: int = 10, 
                                 retry_limit: int = 3,
                                 retry_delay: int = 5) -> List[List[float]]:
        """
        Generate embeddings for a batch of texts using OpenAI API.
        
        Args:
            texts: List of texts to generate embeddings for
            batch_size: Number of texts to process in each API call
            retry_limit: Maximum number of retries for failed API calls
            retry_delay: Delay in seconds between retries
            
        Returns:
            List of vector embeddings
        """
        all_embeddings = []
        
        # Process in batches
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i+batch_size]
            
            # Retry logic
            for attempt in range(retry_limit):
                try:
                    response = self.client.embeddings.create(
                        model=self.embedding_model,
                        input=batch
                    )
                    
                    # Sort embeddings by index to maintain order
                    sorted_embeddings = sorted(response.data, key=lambda x: x.index)
                    batch_embeddings = [item.embedding for item in sorted_embeddings]
                    
                    all_embeddings.extend(batch_embeddings)
                    
                    # Add delay to avoid rate limiting
                    time.sleep(0.5)
                    break
                except Exception as e:
                    logger.warning(f"Error in batch {i//batch_size} (attempt {attempt+1}/{retry_limit}): {e}")
                    
                    if attempt < retry_limit - 1:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Failed to generate embeddings for batch {i//batch_size} after {retry_limit} attempts")
                        # Add empty embeddings as placeholders
                        all_embeddings.extend([[] for _ in range(len(batch))])
        
        return all_embeddings
    
    def process_file(self, filename: str) -> List[Dict[str, Any]]:
        """
        Process a single file of documents and generate embeddings.
        
        Args:
            filename: Name of the file to process
            
        Returns:
            List of documents with embeddings
        """
        logger.info(f"Processing file: {filename}")
        
        try:
            # Load documents
            with open(f"{self.data_dir}/{filename}", "r") as f:
                documents = json.load(f)
            
            if not documents:
                logger.warning(f"No documents found in {filename}")
                return []
            
            # Extract texts
            texts = [doc["text"] for doc in documents]
            
            # Generate embeddings
            logger.info(f"Generating embeddings for {len(texts)} documents")
            embeddings = self.generate_embeddings_batch(texts)
            
            # Add embeddings to documents
            for i, embedding in enumerate(embeddings):
                if embedding:  # Skip empty embeddings (failed API calls)
                    documents[i]["embedding"] = embedding
            
            # Filter out documents without embeddings
            documents_with_embeddings = [doc for doc in documents if "embedding" in doc]
            
            # Save documents with embeddings
            output_filename = f"embedded_{filename}"
            with open(f"{self.output_dir}/{output_filename}", "w") as f:
                json.dump(documents_with_embeddings, f)
            
            logger.info(f"Saved {len(documents_with_embeddings)} documents with embeddings to {output_filename}")
            
            return documents_with_embeddings
        except Exception as e:
            logger.error(f"Error processing file {filename}: {e}")
            return []
    
    def process_all_files(self) -> List[Dict[str, Any]]:
        """
        Process all files in the data directory and generate embeddings.
        
        Returns:
            List of all documents with embeddings
        """
        logger.info("Processing all files")
        
        all_documents = []
        
        # Get all JSON files
        json_files = [f for f in os.listdir(self.data_dir) if f.endswith(".json")]
        
        for filename in tqdm(json_files, desc="Processing files"):
            documents = self.process_file(filename)
            all_documents.extend(documents)
        
        # Save all documents with embeddings
        with open(f"{self.output_dir}/all_embedded_data.json", "w") as f:
            json.dump(all_documents, f)
        
        logger.info(f"Processed {len(all_documents)} total documents with embeddings")
        return all_documents
    
    def process_combined_file(self, filename: str = "all_processed_data.json") -> List[Dict[str, Any]]:
        """
        Process a combined file of all documents and generate embeddings.
        
        Args:
            filename: Name of the combined file
            
        Returns:
            List of all documents with embeddings
        """
        logger.info(f"Processing combined file: {filename}")
        
        try:
            # Load documents
            with open(f"{self.data_dir}/{filename}", "r") as f:
                documents = json.load(f)
            
            if not documents:
                logger.warning(f"No documents found in {filename}")
                return []
            
            # Extract texts
            texts = [doc["text"] for doc in documents]
            
            # Generate embeddings
            logger.info(f"Generating embeddings for {len(texts)} documents")
            embeddings = self.generate_embeddings_batch(texts)
            
            # Add embeddings to documents
            for i, embedding in enumerate(embeddings):
                if embedding:  # Skip empty embeddings (failed API calls)
                    documents[i]["embedding"] = embedding
            
            # Filter out documents without embeddings
            documents_with_embeddings = [doc for doc in documents if "embedding" in doc]
            
            # Save documents with embeddings
            output_filename = f"all_embedded_data.json"
            with open(f"{self.output_dir}/{output_filename}", "w") as f:
                json.dump(documents_with_embeddings, f)
            
            logger.info(f"Saved {len(documents_with_embeddings)} documents with embeddings to {output_filename}")
            
            return documents_with_embeddings
        except Exception as e:
            logger.error(f"Error processing combined file {filename}: {e}")
            return []


if __name__ == "__main__":
    # Create generator
    generator = NBAEmbeddingsGenerator()
    
    # Process combined file
    generator.process_combined_file()
