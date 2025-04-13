"""
Script to generate test data for the Messenger application.
This script is a skeleton for students to implement.
"""
import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def generate_test_data(session):
    logger.info("Generating test data...")

    user_ids = [uuid.uuid4() for _ in range(NUM_USERS)]
    conversations = []

    for _ in range(NUM_CONVERSATIONS):
        user_a, user_b = random.sample(user_ids, 2)
        conversation_id = uuid.uuid4()
        conversations.append((conversation_id, user_a, user_b))

        num_messages = random.randint(10, MAX_MESSAGES_PER_CONVERSATION)
        base_time = datetime.utcnow()

        for i in range(num_messages):
            msg_time = base_time - timedelta(seconds=i * 60)
            sender = random.choice([user_a, user_b])
            content = f"Test message {i} from {sender}"

            # Insert into messages_by_conversation
            session.execute("""
                INSERT INTO messages_by_conversation (conversation_id, message_ts, sender_id, content)
                VALUES (%s, %s, %s, %s)
            """, (conversation_id, msg_time, sender, content))

        # Insert into conversations_by_user for both participants
        for user, peer in [(user_a, user_b), (user_b, user_a)]:
            session.execute("""
                INSERT INTO conversations_by_user (user_id, last_message_ts, conversation_id, peer_id)
                VALUES (%s, %s, %s, %s)
            """, (user, base_time, conversation_id, peer))

    logger.info(f"Generated {len(conversations)} conversations with messages")
    logger.info(f"User IDs range from 1 to {NUM_USERS}")
    logger.info("Use these IDs for testing the API endpoints")

def main():
    """Main function to generate test data."""
    cluster = None
    
    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()
        
        # Generate test data
        generate_test_data(session)
        
        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main() 