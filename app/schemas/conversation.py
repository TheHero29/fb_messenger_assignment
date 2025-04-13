from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from app.schemas.message import MessageResponse
import uuid
class ConversationResponse(BaseModel):
    conversation_id: uuid.UUID = Field(..., description="Unique ID of the conversation")
    peer_id: uuid.UUID = Field(..., description="ID of the peer in the conversation")
    last_message_ts: datetime = Field(..., description="Timestamp of the last message")
    last_message_content: Optional[str] = Field(None, description="Content of the last message")

class ConversationDetail(ConversationResponse):
    messages: List[MessageResponse] = Field(..., description="List of messages in conversation")

class PaginatedConversationRequest(BaseModel):
    page: int = Field(1, description="Page number for pagination")
    limit: int = Field(20, description="Number of items per page")

class PaginatedConversationResponse(BaseModel):
    total: int = Field(..., description="Total number of conversations")
    page: int = Field(..., description="Current page number")
    limit: int = Field(..., description="Number of items per page")
    data: List[ConversationResponse] = Field(..., description="List of conversations")
