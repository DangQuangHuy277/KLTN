package chatmanagement

import (
	"HNLP/be/internal/db"
	"context"
)

type RepositoryImpl struct {
	db db.HDb
}

func NewRepositoryImpl(db db.HDb) *RepositoryImpl {
	return &RepositoryImpl{
		db: db,
	}
}

func (r *RepositoryImpl) GetConversationsByUserID(ctx context.Context, userID int) ([]Conversation, error) {
	var conversations []Conversation
	err := r.db.SelectContext(ctx, &conversations, "SELECT * FROM conversation WHERE user_id = $1", userID)
	if err != nil {
		return nil, err
	}
	return conversations, nil
}

func (r *RepositoryImpl) GetConversationByID(ctx context.Context, conversationID int) (Conversation, error) {
	var conversation Conversation
	err := r.db.GetContext(ctx, &conversation, "SELECT * FROM conversation WHERE id = $1", conversationID)
	if err != nil {
		return Conversation{}, err
	}
	return conversation, nil
}

func (r *RepositoryImpl) CreateConversation(ctx context.Context, conversation *Conversation) (int, error) {
	var id int
	err := r.db.QueryRowx(
		"INSERT INTO conversation (user_id, title) VALUES ($1, $2) RETURNING id",
		conversation.UserID, conversation.Title).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (r *RepositoryImpl) UpdateConversation(ctx context.Context, conversation *Conversation) (int, error) {
	_, err := r.db.ExecContext(ctx, "UPDATE conversation SET title = $1 WHERE id = $2",
		conversation.Title, conversation.ID)
	if err != nil {
		return 0, err
	}
	return conversation.ID, nil
}

func (r *RepositoryImpl) DeleteConversation(ctx context.Context, conversation *Conversation) (bool, error) {
	_, err := r.db.ExecContext(ctx, "DELETE FROM conversation WHERE id = $1", conversation.ID)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *RepositoryImpl) GetMessagesByConversationID(ctx context.Context, conversationID int) ([]Message, error) {
	var messages []Message
	err := r.db.SelectContext(ctx, &messages, "SELECT * FROM message WHERE conversation_id = $1", conversationID)
	if err != nil {
		return nil, err
	}
	return messages, nil
}

func (r *RepositoryImpl) SaveMessage(ctx context.Context, message *Message) (bool, error) {
	_, err := r.db.ExecContext(ctx, "INSERT INTO message (conversation_id, sender_type, content) VALUES ($1, $2, $3)",
		message.ConversationID, message.SenderType, message.Content)
	if err != nil {
		return false, err
	}
	return true, nil
}
