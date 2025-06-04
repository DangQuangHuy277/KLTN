package chatmanagement

import (
	"context"
)

type ServiceImpl struct {
	repo Repository
}

func NewServiceImpl(repo Repository) *ServiceImpl {
	return &ServiceImpl{
		repo: repo,
	}
}

func (s *ServiceImpl) GetConversations(ctx context.Context, req GetConversationsRequest) (GetConversationsResponse, error) {
	conversations, err := s.repo.GetConversationsByUserID(ctx, req.UserId)
	if err != nil {
		return GetConversationsResponse{}, err
	}
	return GetConversationsResponse{Conversations: conversations}, nil
}

func (s *ServiceImpl) EditConversation(ctx context.Context, req EditConversationRequest) (int, error) {
	conversation, err := s.repo.GetConversationByID(ctx, req.ConversationId)
	if err != nil {
		return 0, err
	}

	conversation.Title = req.Title
	id, err := s.repo.UpdateConversation(ctx, &conversation)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (s *ServiceImpl) DeleteConversation(ctx context.Context, id int) (bool, error) {
	conversation, err := s.repo.GetConversationByID(ctx, id)
	if err != nil {
		return false, err
	}

	res, err := s.repo.DeleteConversation(ctx, &conversation)
	if err != nil {
		return false, err
	}
	return res, nil
}

func (s *ServiceImpl) GetMessagesByConversation(ctx context.Context, req GetMessagesRequest) (GetMessagesResponse, error) {
	messages, err := s.repo.GetMessagesByConversationID(ctx, req.ConversationId)
	if err != nil {
		return GetMessagesResponse{}, err
	}
	return GetMessagesResponse{Messages: messages}, nil
}

func (s *ServiceImpl) CreateConversation(ctx context.Context, req CreateConversationRequest) (int, error) {
	conversation := Conversation{
		UserID: req.UserId,
		Title:  req.Title,
	}
	conservationId, err := s.repo.CreateConversation(ctx, &conversation)
	if err != nil {
		return 0, err
	}
	return conservationId, nil
}

func (s *ServiceImpl) CreateMessage(ctx context.Context, req CreateMessageRequest) (CreateMessageResponse, error) {
	// A hack for frontend here, create a conversation if it doesn't exist
	var conversationId int
	var err error

	if req.ConversationId == nil || *req.ConversationId == 0 {
		// Don't redeclare conversationId with :=
		conversationId, err = s.CreateConversation(ctx, CreateConversationRequest{
			UserId: req.UserId,
			Title:  "New Conversation",
		})
		if err != nil {
			return CreateMessageResponse{}, err
		}
	} else {
		conversationId = *req.ConversationId
	}

	message := Message{
		ConversationID: conversationId,
		Content:        req.Content,
		SenderType:     req.Role,
	}

	_, err = s.repo.SaveMessage(ctx, &message)
	if err != nil {
		return CreateMessageResponse{}, err
	}
	return CreateMessageResponse{
		ConversationId: conversationId,
	}, nil
}
