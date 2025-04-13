from fastapi import HTTPException, status
from app.models.cassandra_models import ConversationModel
from app.schemas.conversation import ConversationResponse, PaginatedConversationResponse
from uuid import UUID
from datetime import datetime

class ConversationController:
    """
    Controller for handling conversation operations
    """
    
    async def get_user_conversations(
        self, 
        user_id: UUID, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedConversationResponse:
        """
        Get all conversations for a user with pagination
        
        Args:
            user_id (UUID): ID of the user
            page (int): Page number
            limit (int): Number of conversations per page
            
        Returns:
            PaginatedConversationResponse: Paginated list of conversations
            
        Raises:
            HTTPException: If user not found or access denied
        """
        try:
            # Fetch conversations from the model
            before_ts = datetime.utcnow()  # You can modify this based on your pagination logic
            conversations = await ConversationModel.get_user_conversations(user_id, limit, before_ts)
            
            if not conversations:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No conversations found for this user"
                )
            
            # Prepare paginated response
            paginated_conversations = PaginatedConversationResponse(
                conversations=conversations,
                page=page,
                limit=limit,
                total=len(conversations)  # Assuming you have a way to count the total number of conversations
            )
            
            return paginated_conversations
        
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An error occurred while retrieving conversations: {str(e)}"
            )
    
    async def get_conversation(self, conversation_id: UUID) -> ConversationResponse:
        """
        Get a specific conversation by ID
        
        Args:
            conversation_id (UUID): ID of the conversation
            
        Returns:
            ConversationResponse: Conversation details
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        try:
            # Fetch conversation details from the model
            conversation = await ConversationModel.get_conversation(conversation_id)
            
            if not conversation:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Conversation not found"
                )
            
            # Prepare the conversation response
            conversation_response = ConversationResponse(
                conversation_id=conversation["conversation_id"],
                peer_id=conversation["peer_id"]
            )
            
            return conversation_response
        
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An error occurred while retrieving the conversation: {str(e)}"
            )

    async def create_or_get_conversation(self, user_a: UUID, user_b: UUID) -> UUID:
        """
        Get an existing conversation between two users or create a new one
        
        Args:
            user_a (UUID): The ID of the first user
            user_b (UUID): The ID of the second user
            
        Returns:
            UUID: The conversation ID
            
        Raises:
            HTTPException: If there was an error in creating or retrieving the conversation
        """
        try:
            # Use the model to get or create the conversation
            conversation_id = await ConversationModel.create_or_get_conversation(user_a, user_b)
            
            return conversation_id
        
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An error occurred while creating or retrieving the conversation: {str(e)}"
            )
