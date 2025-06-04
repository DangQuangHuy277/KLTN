package chatmanagement

import (
	"HNLP/be/internal/auth"
	"HNLP/be/internal/middleware"
	"github.com/gin-gonic/gin"
	"strconv"
)

type Controller struct {
	service Service
}

func NewController(service Service) *Controller {
	return &Controller{service: service}
}

// GetConversations handler
func (c *Controller) GetConversations(ctx *gin.Context) {
	// Get the user ID from the JWT token
	userIdRaw, ok := ctx.Get("userId")
	if !ok {
		ctx.JSON(401, gin.H{"error": "user ID not found"})
		return
	}

	userId, ok := userIdRaw.(float64)
	if !ok {
		ctx.JSON(500, gin.H{"error": "invalid user ID type"})
		return
	}

	getConversationsRequest := GetConversationsRequest{
		UserId: int(userId),
	}

	response, err := c.service.GetConversations(ctx.Request.Context(), getConversationsRequest)
	if err != nil {
		ctx.JSON(500, gin.H{"error": "failed to get conversations"})
		return
	}

	ctx.JSON(200, response.Conversations)
}

func (c *Controller) EditConversation(ctx *gin.Context) {
	conversationId, err := strconv.Atoi(ctx.Param("conversationId"))
	if err != nil {
		ctx.JSON(400, gin.H{"error": "invalid conversation ID"})
		return
	}

	var request EditConversationRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		ctx.JSON(400, gin.H{"error": "invalid request"})
		return
	}

	request.ConversationId = conversationId

	response, err := c.service.EditConversation(ctx.Request.Context(), request)
	if err != nil {
		ctx.JSON(500, gin.H{"error": "failed to edit conversation"})
		return
	}

	ctx.JSON(200, response)
}

func (c *Controller) DeleteConversation(ctx *gin.Context) {
	conversationId, err := strconv.Atoi(ctx.Param("conversationId"))
	if err != nil {
		ctx.JSON(400, gin.H{"error": "invalid conversation ID"})
		return
	}

	response, err := c.service.DeleteConversation(ctx.Request.Context(), conversationId)
	if err != nil {
		ctx.JSON(500, gin.H{"error": "failed to edit conversation"})
		return
	}

	ctx.JSON(200, response)
}

func (c *Controller) GetMessagesByConversation(ctx *gin.Context) {
	conversationId, err := strconv.Atoi(ctx.Param("conversationId"))
	if err != nil {
		ctx.JSON(400, gin.H{"error": "invalid conversation ID"})
		return
	}

	request := GetMessagesRequest{
		ConversationId: conversationId,
	}

	response, err := c.service.GetMessagesByConversation(ctx.Request.Context(), request)
	if err != nil {
		ctx.JSON(500, gin.H{"error": "failed to get messages"})
		return
	}

	ctx.JSON(200, response)
}

func (c *Controller) CreateConversation(ctx *gin.Context) {
	// Get the user ID from the JWT token
	userIdRaw, ok := ctx.Get("userId")
	if !ok {
		ctx.JSON(401, gin.H{"error": "user ID not found"})
		return
	}

	userId, ok := userIdRaw.(float64)
	if !ok {
		ctx.JSON(500, gin.H{"error": "invalid user ID type"})
		return
	}

	var request CreateConversationRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		ctx.JSON(400, gin.H{"error": "invalid request"})
		return
	}

	request.UserId = int(userId)

	response, err := c.service.CreateConversation(ctx.Request.Context(), request)
	if err != nil {
		ctx.JSON(500, gin.H{"error": "failed to create conversation"})
		return
	}

	ctx.JSON(200, response)
}

func (c *Controller) CreateMessage(ctx *gin.Context) {
	// Get the user ID from the JWT token
	userIdRaw, ok := ctx.Get("userId")
	if !ok {
		ctx.JSON(401, gin.H{"error": "user ID not found"})
		return
	}

	userId, ok := userIdRaw.(float64)
	if !ok {
		ctx.JSON(500, gin.H{"error": "invalid user ID type"})
		return
	}

	var request CreateMessageRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		ctx.JSON(400, gin.H{"error": "invalid request"})
		return
	}

	request.UserId = int(userId)

	response, err := c.service.CreateMessage(ctx.Request.Context(), request)
	if err != nil {
		ctx.JSON(500, gin.H{"error": "failed to create message"})
		return
	}

	ctx.JSON(200, response)
}

func (c *Controller) RegisterRoutes(router *gin.Engine, jwtService *auth.ServiceImpl) {
	router.GET("/api/v1/conversations", middleware.Authenticate(jwtService), c.GetConversations)
	router.POST("/api/v1/conversations", middleware.Authenticate(jwtService), c.CreateConversation)
	router.PUT("/api/v1/conversations/:conversationId", middleware.Authenticate(jwtService), c.EditConversation)
	router.DELETE("/api/v1/conversations/:conversationId", middleware.Authenticate(jwtService), c.DeleteConversation)
	router.GET("/api/v1/conversations/:conversationId/messages", middleware.Authenticate(jwtService), c.GetMessagesByConversation)
	router.POST("/api/v1/messages", middleware.Authenticate(jwtService), c.CreateMessage)
}
