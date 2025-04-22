#!/usr/bin/env python3
"""
NBA Turso (libSQL) Connector for RAG-LLM Chatbot

Provides functionality to upload embedded NBA data to a Turso database that
supports native vector search via libSQL. The connector mirrors the public
interface of `NBAMongoDBConnector` so it can be dropped into the existing
population pipeline with minimal code changes.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any, Dict, List

import libsql_experimental as libsql  # type: ignore
from dotenv import load_dotenv
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("nba_turso_connector.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

load_dotenv()


class NBATursoConnector:
    """Upload embedded NBA documents to a Turso/libSQL database."""

    def __init__(
        self,
        data_dir: str = "nba_embeddings",
        db_path: str = "nba_docs.db",
        table_name: str = "documents",
        index_name: str = "embedding_idx",
    ) -> None:
        self.data_dir = data_dir
        self.local_db_path = db_path
        self.table_name = table_name
        self.index_name = index_name

        self.url = os.getenv("TURSO_DATABASE_URL")
        self.auth_token = os.getenv("TURSO_AUTH_TOKEN")

        if not self.url or not self.auth_token:
            logger.error("TURSO_DATABASE_URL and TURSO_AUTH_TOKEN must be set in environment")
            raise ValueError("Missing Turso credentials")

        self.conn: libsql.Connection | None = None
        self.cur: libsql.Cursor | None = None

        logger.info("NBA Turso Connector initialised")

    def connect(self) -> bool:
        """Connect and ensure schema/index exist."""
        try:
            self.conn = libsql.connect(
                self.local_db_path, sync_url=self.url, auth_token=self.auth_token
            )
            self.conn.sync()
            self.cur = self.conn.cursor()

            self.cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    embedding BLOB NOT NULL,
                    metadata TEXT
                );
                """
            )

            self.cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self.index_name}
                ON {self.table_name}(libsql_vector_idx(embedding));
                """
            )
            self.conn.commit()
            logger.info("Connected to Turso and ensured schema/index")
            return True
        except Exception as e:
            logger.error(f"Error connecting to Turso: {e}")
            return False

    def disconnect(self) -> None:
        if self.conn:
            try:
                self.conn.sync()
            except Exception:
                pass
            self.conn.close()
            logger.info("Disconnected from Turso")

    def clear_collection(self) -> bool:
        if not self.cur:
            return False
        try:
            self.cur.execute(f"DELETE FROM {self.table_name};")
            self.conn.commit()
            logger.info("Cleared Turso documents table")
            return True
        except Exception as e:
            logger.error(f"Error clearing Turso table: {e}")
            return False

    def _insert_document(self, doc: Dict[str, Any]) -> None:
        if not self.cur:
            raise RuntimeError("Not connected")
        doc_id = str(doc.get("_id") or uuid.uuid4())
        text_val = doc.get("text", "")
        embedding = doc.get("embedding")
        if not isinstance(embedding, list):
            raise ValueError("Document missing 'embedding' list")
        embedding_json = json.dumps(embedding)  # vector32() accepts JSON array
        metadata_json = json.dumps(doc.get("metadata", {}))

        self.cur.execute(
            f"INSERT OR REPLACE INTO {self.table_name} (id, text, embedding, metadata) "
            "VALUES (?, ?, vector32(?), ?);",
            (doc_id, text_val, embedding_json, metadata_json),
        )

    def upload_documents(
        self,
        documents: List[Dict[str, Any]],
        batch_size: int = 100,
        clear_first: bool = False,
    ) -> int:
        if not documents:
            logger.warning("No documents supplied for upload")
            return 0

        if not self.connect():
            return 0

        if clear_first:
            self.clear_collection()

        total_uploaded = 0
        try:
            for i in range(0, len(documents), batch_size):
                batch = documents[i : i + batch_size]
                for doc in batch:
                    try:
                        self._insert_document(doc)
                    except Exception as e:
                        logger.error(f"Failed to insert document: {e}")
                self.conn.commit()
                total_uploaded += len(batch)
                logger.info(
                    f"Uploaded batch {i // batch_size + 1}/"
                    f"{(len(documents) - 1) // batch_size + 1}: {len(batch)} docs"
                )
        finally:
            self.disconnect()
        return total_uploaded

    # Convenience wrappers matching MongoDB connector
    def upload_file(self, filename: str, clear_first: bool = False) -> int:
        abs_path = os.path.join(self.data_dir, filename)
        if not os.path.exists(abs_path):
            logger.error(f"File not found: {abs_path}")
            return 0
        try:
            with open(abs_path, "r") as f:
                docs = json.load(f)
        except Exception as e:
            logger.error(f"Error reading {abs_path}: {e}")
            return 0
        if not isinstance(docs, list):
            logger.error(f"Expected list in {abs_path}")
            return 0
        return self.upload_documents(docs, clear_first=clear_first)

    def upload_combined_file(
        self, filename: str = "all_embedded_data.json", clear_first: bool = False
    ) -> int:
        return self.upload_file(filename, clear_first=clear_first)


if __name__ == "__main__":
    connector = NBATursoConnector()
    uploaded = connector.upload_combined_file(clear_first=False)
    print(f"Uploaded {uploaded} documents to Turso")
