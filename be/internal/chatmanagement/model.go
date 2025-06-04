package chatmanagement

import "time"

// SenderType represents who sent a message
type SenderType string

const (
	// SenderTypeUser indicates the message was sent by a user
	SenderTypeUser SenderType = "user"
	// SenderTypeBot indicates the message was sent by the bot
	SenderTypeBot SenderType = "bot"
)

type Conversation struct {
	ID        int        `db:"id" json:"id"`
	UserID    int        `db:"user_id" json:"user_id"`
	Title     string     `db:"title" json:"title"`
	Disabled  bool       `db:"disabled" json:"disabled"`
	CreatedAt time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt time.Time  `db:"updated_at" json:"updated_at"`
	DeletedAt *time.Time `db:"deleted_at" json:"deleted_at,omitempty"`
}

type Message struct {
	ID             int        `db:"id" json:"id"`
	ConversationID int        `db:"conversation_id" json:"conversation_id"`
	SenderType     SenderType `db:"sender_type" json:"role"`
	Content        string     `db:"content" json:"content"`
	CreatedAt      time.Time  `db:"created_at" json:"created_at"`
	DeletedAt      *time.Time `db:"deleted_at" json:"deleted_at,omitempty"`
}
