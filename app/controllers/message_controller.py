from typing import Optional
from datetime import datetime
from fastapi import HTTPException, status
from app.models.cassandra_models import MessageModel
from app.schemas.message import MessageCreate, MessageResponse, PaginatedMessageResponse
from uuid import UUID

class MessageController:
    """
    Controller for handling message operations
    """
    
    async def send_message(self, message_data: MessageCreate) -> MessageResponse:
        """
        Send a message from one user to another
        
        Args:
            message_data: The message data including content, sender_id, and receiver_id
            
        Returns:
            MessageResponse: The created message with metadata
        
        Raises:
            HTTPException: If message sending fails
        """
        try:
            # Create the message using the MessageModel
            message_id = await MessageModel.create_message(
                sender_id=message_data.sender_id,
                receiver_id=message_data.receiver_id,
                content=message_data.content,
                timestamp=datetime.utcnow()
            )
            
            # Fetch the created message details
            message = await MessageModel.get_message_by_id(message_id)
            
            if not message:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Message creation failed"
                )
            
            return MessageResponse(
                message_id=message["message_id"],
                sender_id=message["sender_id"],
                receiver_id=message["receiver_id"],
                content=message["content"],
                timestamp=message["timestamp"]
            )
        
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An error occurred while sending the message: {str(e)}"
            )
    
    async def get_conversation_messages(
        self, 
        conversation_id: UUID, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get all messages in a conversation with pagination
        
        Args:
            conversation_id (UUID): ID of the conversation
            page (int): Page number
            limit (int): Number of messages per page
            
        Returns:
            PaginatedMessageResponse: Paginated list of messages
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        try:
            # Fetch messages from the model
            messages = await MessageModel.get_conversation_messages(
                conversation_id=conversation_id,
                page=page,
                limit=limit
            )
            
            if not messages:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No messages found for this conversation"
                )
            
            # Return the paginated response
            return PaginatedMessageResponse(
                messages=messages,
                page=page,
                limit=limit,
                total=len(messages)  # Assuming you have a way to count the total number of messages
            )
        
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An error occurred while retrieving messages: {str(e)}"
            )
    
    async def get_messages_before_timestamp(
        self, 
        conversation_id: UUID, 
        before_timestamp: datetime,
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get messages in a conversation before a specific timestamp with pagination
        
        Args:
            conversation_id (UUID): ID of the conversation
            before_timestamp (datetime): Get messages before this timestamp
            page (int): Page number
            limit (int): Number of messages per page
            
        Returns:
            PaginatedMessageResponse: Paginated list of messages
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        try:
            # Fetch messages from the model
            messages = await MessageModel.get_messages_before_timestamp(
                conversation_id=conversation_id,
                before_timestamp=before_timestamp,
                page=page,
                limit=limit
            )
            
            if not messages:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No messages found before the given timestamp"
                )
            
            # Return the paginated response
            return PaginatedMessageResponse(
                messages=messages,
                page=page,
                limit=limit,
                total=len(messages)  # Assuming you have a way to count the total number of messages
            )
        
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An error occurred while retrieving messages: {str(e)}"
            )
