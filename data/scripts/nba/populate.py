#!/usr/bin/env python3
"""
OpenMuse Population Script for RAG-LLM Chatbot

This script orchestrates the entire process of collecting, processing,
embedding, and uploading NBA data to a MongoDB Atlas vector database.
"""

import os
import argparse
import logging
import time
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

from dotenv import load_dotenv
from tqdm import tqdm

from collector import NBADataCollector
from processor import NBADataProcessor
from embeddings import NBAEmbeddingsGenerator
from connector import NBAMongoDBConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nba_database_populator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

class NBADatabasePopulator:
    """
    Orchestrates the entire process of collecting, processing,
    embedding, and uploading NBA data to a MongoDB Atlas vector database.
    """
    
    def __init__(self,
                raw_data_dir: str = "nba_data",
                processed_data_dir: str = "nba_processed_data",
                embeddings_dir: str = "nba_embeddings",
                db_name: str = "OpenMuse",
                collection_name: str = "nba",
                index_name: str = "vector_index"):
        """
        Initialize the NBA database populator.
        
        Args:
            raw_data_dir: Directory to store raw NBA data
            processed_data_dir: Directory to store processed NBA data
            embeddings_dir: Directory to store embedded NBA data
            db_name: MongoDB database name
            collection_name: MongoDB collection name
            index_name: Vector index name
        """
        self.raw_data_dir = raw_data_dir
        self.processed_data_dir = processed_data_dir
        self.embeddings_dir = embeddings_dir
        self.db_name = db_name
        self.collection_name = collection_name
        self.index_name = index_name
        
        os.makedirs(raw_data_dir, exist_ok=True)
        os.makedirs(processed_data_dir, exist_ok=True)
        os.makedirs(embeddings_dir, exist_ok=True)
        
        self.collector = NBADataCollector(output_dir=raw_data_dir)

        self.collector.collect_all_teams()
        self.collector.collect_all_players()

        self.processor = NBADataProcessor(data_dir=raw_data_dir, output_dir=processed_data_dir)
        self.embedder = NBAEmbeddingsGenerator(data_dir=processed_data_dir, output_dir=embeddings_dir)
        self.connector = NBAMongoDBConnector(
            data_dir=embeddings_dir,
            db_name=db_name,
            collection_name=collection_name,
            index_name=index_name
        )
        
        logger.info("NBA Database Populator initialized")
    
    def check_environment_variables(self) -> bool:
        """
        Check if required environment variables are set.
        
        Returns:
            True if all required variables are set, False otherwise
        """
        required_vars = ["MONGODB_URI", "OPENAI_API_KEY"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
            logger.error("Please set these variables in a .env file or environment")
            return False
        
        logger.info("All required environment variables are set")
        return True
    
    def collect_data(self, 
                   player_limit: Optional[int] = None,
                   seasons: List[str] = ["2023-24"]) -> bool:
        """
        Collect NBA data.
        
        Args:
            player_limit: Optional limit on number of players to process
            seasons: List of NBA seasons to collect data for
            
        Returns:
            True if data collection successful, False otherwise
        """
        try:
            logger.info("Starting data collection")
            start_time = time.time()
            
            self.collector.run_collection(
                player_limit=player_limit,
                seasons=seasons
            )
            
            elapsed_time = time.time() - start_time
            logger.info(f"Data collection completed in {elapsed_time:.2f} seconds")
            return True
        except Exception as e:
            logger.error(f"Error collecting data: {e}")
            return False
    
    def process_data(self, player_limit: Optional[int] = None) -> bool:
        """
        Process NBA data.
        
        Args:
            player_limit: Optional limit on number of players to process
            
        Returns:
            True if data processing successful, False otherwise
        """
        try:
            logger.info("Starting data processing")
            start_time = time.time()
            
            self.processor.process_all_data(player_limit=player_limit)
            
            elapsed_time = time.time() - start_time
            logger.info(f"Data processing completed in {elapsed_time:.2f} seconds")
            return True
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            return False
    
    def generate_embeddings(self) -> bool:
        """
        Generate embeddings for processed NBA data.
        
        Returns:
            True if embedding generation successful, False otherwise
        """
        try:
            logger.info("Starting embedding generation")
            start_time = time.time()
            
            self.embedder.process_combined_file()
            
            elapsed_time = time.time() - start_time
            logger.info(f"Embedding generation completed in {elapsed_time:.2f} seconds")
            return True
        except Exception as e:
            logger.error(f"Error generating embeddings: {e}")
            return False
    
    def upload_to_database(self, clear_first: bool = True) -> bool:
        """
        Upload embedded NBA data to MongoDB Atlas.
        
        Args:
            clear_first: Whether to clear the collection before uploading
            
        Returns:
            True if database upload successful, False otherwise
        """
        try:
            logger.info("Starting database upload")
            start_time = time.time()
            
            uploaded = self.connector.upload_combined_file(clear_first=clear_first)
            
            if uploaded > 0:
                elapsed_time = time.time() - start_time
                logger.info(f"Database upload completed in {elapsed_time:.2f} seconds")
                logger.info(f"Uploaded {uploaded} documents to MongoDB Atlas")
                return True
            else:
                logger.error("No documents were uploaded to MongoDB Atlas")
                return False
        except Exception as e:
            logger.error(f"Error uploading to database: {e}")
            return False
    
    def run_full_pipeline(self, 
                        player_limit: Optional[int] = None,
                        seasons: List[str] = ["2023-24"],
                        clear_first: bool = True) -> bool:
        """
        Run the full pipeline: collect, process, embed, and upload.
        
        Args:
            player_limit: Optional limit on number of players to process
            seasons: List of NBA seasons to collect data for
            clear_first: Whether to clear the collection before uploading
            
        Returns:
            True if full pipeline successful, False otherwise
        """
        logger.info("Starting full pipeline")
        start_time = time.time()
        
        if not self.check_environment_variables():
            return False
        
        if not self.collect_data(player_limit=player_limit, seasons=seasons):
            logger.error("Data collection failed")
            return False
        
        if not self.process_data(player_limit=player_limit):
            logger.error("Data processing failed")
            return False
        
        if not self.generate_embeddings():
            logger.error("Embedding generation failed")
            return False
        
        if not self.upload_to_database(clear_first=clear_first):
            logger.error("Database upload failed")
            return False
        
        elapsed_time = time.time() - start_time
        logger.info(f"Full pipeline completed in {elapsed_time:.2f} seconds")
        return True
    
    def run_from_processed_data(self, clear_first: bool = True) -> bool:
        """
        Run pipeline starting from processed data: embed and upload.
        
        Args:
            clear_first: Whether to clear the collection before uploading
            
        Returns:
            True if pipeline successful, False otherwise
        """
        logger.info("Starting pipeline from processed data")
        start_time = time.time()
        
        if not self.check_environment_variables():
            return False
        
        if not self.generate_embeddings():
            logger.error("Embedding generation failed")
            return False
        
        if not self.upload_to_database(clear_first=clear_first):
            logger.error("Database upload failed")
            return False
        
        elapsed_time = time.time() - start_time
        logger.info(f"Pipeline completed in {elapsed_time:.2f} seconds")
        return True
    
    def run_from_embeddings(self, clear_first: bool = True) -> bool:
        """
        Run pipeline starting from embeddings: upload only.
        
        Args:
            clear_first: Whether to clear the collection before uploading
            
        Returns:
            True if pipeline successful, False otherwise
        """
        logger.info("Starting pipeline from embeddings")
        start_time = time.time()
        
        if not self.check_environment_variables():
            return False
        
        if not self.upload_to_database(clear_first=clear_first):
            logger.error("Database upload failed")
            return False
        
        elapsed_time = time.time() - start_time
        logger.info(f"Pipeline completed in {elapsed_time:.2f} seconds")
        return True


def main():
    """
    Main function to run the NBA database populator.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="NBA Database Populator")
    parser.add_argument("--mode", type=str, default="full",
                        choices=["full", "from_processed", "from_embeddings"],
                        help="Pipeline mode: full, from_processed, or from_embeddings")
    parser.add_argument("--player-limit", type=int, default=None,
                        help="Limit number of players to process")
    parser.add_argument("--seasons", type=str, nargs="+", default=["2023-24"],
                        help="NBA seasons to collect data for")
    parser.add_argument("--no-clear", action="store_true",
                        help="Don't clear the collection before uploading")
    parser.add_argument("--db-name", type=str, default="OpenMuse",
                        help="MongoDB database name")
    parser.add_argument("--collection-name", type=str, default="nba",
                        help="MongoDB collection name")
    parser.add_argument("--index-name", type=str, default="vector_index",
                        help="Vector index name")
    
    args = parser.parse_args()
    
    # Create populator
    populator = NBADatabasePopulator(
        db_name=args.db_name,
        collection_name=args.collection_name,
        index_name=args.index_name
    )
    
    # Run pipeline
    if args.mode == "full":
        success = populator.run_full_pipeline(
            player_limit=args.player_limit,
            seasons=args.seasons,
            clear_first=not args.no_clear
        )
    elif args.mode == "from_processed":
        success = populator.run_from_processed_data(
            clear_first=not args.no_clear
        )
    elif args.mode == "from_embeddings":
        success = populator.run_from_embeddings(
            clear_first=not args.no_clear
        )
    else:
        logger.error(f"Invalid mode: {args.mode}")
        return
    
    if success:
        logger.info("NBA database population completed successfully")
    else:
        logger.error("NBA database population failed")


if __name__ == "__main__":
    main()
