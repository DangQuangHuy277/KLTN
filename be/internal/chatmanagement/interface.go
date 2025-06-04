package chatmanagement

import (
	"context"
)

type Service interface {
	GetConversations(ctx context.Context, req GetConversationsRequest) (GetConversationsResponse, error)
	EditConversation(ctx context.Context, req EditConversationRequest) (int, error)
	DeleteConversation(ctx context.Context, id int) (bool, error)
	CreateConversation(ctx context.Context, request CreateConversationRequest) (int, error)
	GetMessagesByConversation(ctx context.Context, req GetMessagesRequest) (GetMessagesResponse, error)
	CreateMessage(ctx context.Context, req CreateMessageRequest) (CreateMessageResponse, error)
}

type Repository interface {
	GetConversationsByUserID(ctx context.Context, userID int) ([]Conversation, error)
	GetConversationByID(ctx context.Context, id int) (Conversation, error)
	CreateConversation(ctx context.Context, c *Conversation) (int, error)
	UpdateConversation(ctx context.Context, c *Conversation) (int, error)
	DeleteConversation(ctx context.Context, c *Conversation) (bool, error)
	GetMessagesByConversationID(ctx context.Context, conversationId int) ([]Message, error)
	SaveMessage(ctx context.Context, m *Message) (bool, error)
}

type GetConversationsRequest struct {
	UserId int `json:"user_id" form:"user_id" uri:"user_id"`
}

type GetConversationsResponse struct {
	Conversations []Conversation `json:"conversations"`
}

type EditConversationRequest struct {
	ConversationId int    `json:"conversation_id" form:"conversation_id" uri:"conversation_id"`
	Title          string `json:"title" form:"title" uri:"title"`
}

type GetMessagesRequest struct {
	ConversationId int `json:"conversation_id" form:"conversation_id" uri:"conversation_id"`
}

type GetMessagesResponse struct {
	Messages []Message `json:"messages"`
}

type CreateConversationRequest struct {
	UserId int    `json:"user_id" form:"user_id" uri:"user_id"`
	Title  string `json:"title" form:"title" uri:"title"`
}

type CreateMessageRequest struct {
	ConversationId *int       `json:"conversation_id" form:"conversation_id" uri:"conversation_id" omitempty:"true"`
	Content        string     `json:"content" form:"content" uri:"content"`
	Role           SenderType `json:"role" form:"role" uri:"role"`
	UserId         int        `json:"user_id" omitempty:"true" form:"user_id" uri:"user_id"`
}

type CreateMessageResponse struct {
	ConversationId int `json:"conversation_id"`
}
