import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
from app.db.cassandra_client import cassandra_client

class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    """

    @staticmethod
    async def get_user_conversations(
        user_id: uuid.UUID, 
        limit: int, 
        before_ts: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get conversations for a user with pagination.
        
        Args:
            user_id (uuid.UUID): The ID of the user.
            limit (int): The number of conversations to fetch.
            before_ts (Optional[datetime]): Get conversations before this timestamp.

        Returns:
            List[Dict]: A list of conversations.
        """
        query = """
            SELECT conversation_id, peer_id, last_message_ts
            FROM conversations_by_user
            WHERE user_id = %s
            AND last_message_ts < %s
            ORDER BY last_message_ts DESC
            LIMIT %s
        """
        result = await cassandra_client.execute(
            query, 
            (user_id, before_ts if before_ts else datetime.utcnow(), limit)
        )
        return [{"conversation_id": row.conversation_id, "peer_id": row.peer_id, "last_message_ts": row.last_message_ts} for row in result]

    @staticmethod
    async def get_conversation(conversation_id: uuid.UUID) -> Dict[str, Any]:
        """
        Get a conversation by ID.
        
        Args:
            conversation_id (uuid.UUID): The ID of the conversation.

        Returns:
            Dict: A conversation object.
        """
        query = """
            SELECT conversation_id, peer_id
            FROM conversations_by_user
            WHERE conversation_id = %s
        """
        result = await cassandra_client.execute(query, (conversation_id,))
        return {
            "conversation_id": result[0].conversation_id,
            "peer_id": result[0].peer_id
        } if result else {}

    @staticmethod
    async def create_or_get_conversation(
        user_a: uuid.UUID, 
        user_b: uuid.UUID
    ) -> uuid.UUID:
        """
        Get an existing conversation between two users or create a new one.
        
        Args:
            user_a (uuid.UUID): The ID of the first user.
            user_b (uuid.UUID): The ID of the second user.

        Returns:
            uuid.UUID: The conversation ID.
        """
        # Check if conversation exists between user_a and user_b
        query = """
            SELECT conversation_id 
            FROM conversations_by_user 
            WHERE user_id IN (%s, %s)
            LIMIT 1
        """
        result = await cassandra_client.execute(query, (user_a, user_b))

        if result:
            return result[0].conversation_id

        # Create a new conversation if it doesn't exist
        conversation_id = uuid.uuid4()
        now = datetime.utcnow()
        
        # Insert the conversation for both users
        await cassandra_client.execute("""
            INSERT INTO conversations_by_user (user_id, last_message_ts, conversation_id, peer_id)
            VALUES (%s, %s, %s, %s)
        """, (user_a, now, conversation_id, user_b))
        
        await cassandra_client.execute("""
            INSERT INTO conversations_by_user (user_id, last_message_ts, conversation_id, peer_id)
            VALUES (%s, %s, %s, %s)
        """, (user_b, now, conversation_id, user_a))

        return conversation_id

class MessageModel:
    """
    Message model for interacting with the messages table.
    """

    @staticmethod
    async def create_message(
        sender_id: uuid.UUID,
        conversation_id: uuid.UUID,
        content: str,
        timestamp: Optional[datetime] = None
    ) -> uuid.UUID:
        """
        Create a new message in a conversation.
        
        Args:
            sender_id (uuid.UUID): The ID of the sender.
            conversation_id (uuid.UUID): The conversation ID.
            content (str): The content of the message.
            timestamp (Optional[datetime]): The timestamp of the message (default is now).

        Returns:
            uuid.UUID: The ID of the created message.
        """
        message_id = uuid.uuid4()
        timestamp = timestamp or datetime.utcnow()

        query = """
            INSERT INTO messages (message_id, conversation_id, sender_id, content, timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """
        await cassandra_client.execute(
            query, 
            (message_id, conversation_id, sender_id, content, timestamp)
        )
        
        # Optionally, update the conversation's last_message_ts
        await cassandra_client.execute("""
            UPDATE conversations_by_user
            SET last_message_ts = %s
            WHERE conversation_id = %s
        """, (timestamp, conversation_id))

        return message_id

    @staticmethod
    async def get_conversation_messages(
        conversation_id: uuid.UUID,
        limit: int = 20,
        before_ts: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get messages for a specific conversation with pagination.
        
        Args:
            conversation_id (uuid.UUID): The conversation ID.
            limit (int): The number of messages to fetch.
            before_ts (Optional[datetime]): Fetch messages before this timestamp.

        Returns:
            List[Dict]: A list of message objects.
        """
        query = """
            SELECT message_id, sender_id, content, timestamp
            FROM messages
            WHERE conversation_id = %s
            AND timestamp < %s
            ORDER BY timestamp DESC
            LIMIT %s
        """
        result = await cassandra_client.execute(
            query, 
            (conversation_id, before_ts or datetime.utcnow(), limit)
        )
        return [
            {
                "message_id": row.message_id,
                "sender_id": row.sender_id,
                "content": row.content,
                "timestamp": row.timestamp
            } for row in result
        ]

    @staticmethod
    async def get_messages_before_timestamp(
        conversation_id: uuid.UUID,
        before_timestamp: datetime,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get messages in a conversation before a specific timestamp.
        
        Args:
            conversation_id (uuid.UUID): The conversation ID.
            before_timestamp (datetime): Get messages before this timestamp.
            limit (int): The number of messages to fetch.

        Returns:
            List[Dict]: A list of message objects.
        """
        return await MessageModel.get_conversation_messages(
            conversation_id, limit, before_timestamp
        )
