from fastapi import APIRouter, Depends, Query, Path
from uuid import UUID
from datetime import datetime
from app.controllers.conversation_controller import ConversationController
from app.schemas.conversation import (
    ConversationResponse,
    PaginatedConversationResponse
)

router = APIRouter(prefix="/api/conversations", tags=["Conversations"])

@router.get("/user/{user_id}", response_model=PaginatedConversationResponse)
async def get_user_conversations(
    user_id: UUID = Path(..., description="UUID of the user"),
    limit: int = Query(20, description="Max conversations to return"),
    before: datetime | None = Query(None, description="Return conversations before this timestamp"),
    conversation_controller: ConversationController = Depends()
) -> PaginatedConversationResponse:
    """
    Get all conversations for a user before a timestamp
    """
    return await conversation_controller.get_user_conversations(
        user_id=user_id,
        limit=limit,
        before_ts=before
    )

@router.get("/{conversation_id}", response_model=ConversationResponse)
async def get_conversation(
    conversation_id: UUID = Path(..., description="UUID of the conversation"),
    conversation_controller: ConversationController = Depends()
) -> ConversationResponse:
    """
    Get a specific conversation by ID
    """
    return await conversation_controller.get_conversation(conversation_id=conversation_id)
