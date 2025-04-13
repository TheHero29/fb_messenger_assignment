from fastapi import APIRouter, Depends, Query, Path, Body
from typing import Optional
from datetime import datetime
from uuid import UUID

from app.controllers.message_controller import MessageController
from app.schemas.message import (
    MessageCreate, 
    MessageResponse, 
    PaginatedMessageResponse
)

router = APIRouter(prefix="/api/messages", tags=["Messages"])

@router.post("/", response_model=MessageResponse, status_code=201)
async def send_message(
    message: MessageCreate = Body(...),
    message_controller: MessageController = Depends()
) -> MessageResponse:
    """
    Send a message from one user to another
    """
    return await message_controller.send_message(message)

@router.get("/conversation/{conversation_id}", response_model=PaginatedMessageResponse)
async def get_conversation_messages(
    conversation_id: UUID = Path(..., description="UUID of the conversation"),
    limit: int = Query(20, description="Number of messages per request"),
    before: Optional[datetime] = Query(None, description="Timestamp to get messages before"),
    message_controller: MessageController = Depends()
) -> PaginatedMessageResponse:
    """
    Get all messages in a conversation with pagination before a timestamp
    """
    return await message_controller.get_conversation_messages(
        conversation_id=conversation_id,
        limit=limit,
        before_ts=before
    )

@router.get("/conversation/{conversation_id}/before", response_model=PaginatedMessageResponse)
async def get_messages_before_timestamp(
    conversation_id: UUID = Path(..., description="UUID of the conversation"),
    before_timestamp: datetime = Query(..., description="Get messages before this timestamp"),
    limit: int = Query(20, description="Number of messages per request"),
    message_controller: MessageController = Depends()
) -> PaginatedMessageResponse:
    """
    Get messages in a conversation before a specific timestamp
    """
    return await message_controller.get_messages_before_timestamp(
        conversation_id=conversation_id,
        before_timestamp=before_timestamp,
        limit=limit
    )
