package chatbot

import (
	"HNLP/be/internal/auth"
	"HNLP/be/internal/middleware"
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
)

type ChatController struct {
	chatService *ChatService
}

func NewChatController(chatService *ChatService) *ChatController {
	return &ChatController{chatService: chatService}
}

//	func (cc *ChatController) QueryByChat(context *gin.Context) {
//		var request QueryRequest
//		if err := context.BindJSON(&request); err != nil {
//			context.JSON(400, gin.H{"error": "Invalid request"})
//			return
//		}
//		// Process the message using the chat service
//		result, err := cc.chatService.QueryByChat(&request)
//		if err != nil {
//			context.JSON(500, gin.H{"error": "Failed to process message"})
//			return
//		}
//		context.JSON(200, result)
//	}
func (cc *ChatController) ChatStreamHandler(ctx *gin.Context) {
	// 1. Set SSE Headers
	w := ctx.Writer
	w.Header().Set("Access-Control-Allow-Origin", "*") // Adjust for production
	w.Header().Set("Access-Control-Expose-Headers", "Content-Type")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// 2. Decode Request Body
	var request ChatRequest
	if err := ctx.ShouldBindJSON(&request); err != nil { // Use Gin's binding
		ctx.String(http.StatusBadRequest, "Invalid request: %v", err)
		return
	}
	//
	//// 3. Get user data from middleware (set earlier)
	userId, ok := ctx.Get("userId")
	specificId, ok := ctx.Get("specificId")
	userRole, ok := ctx.Get("userRole")
	if !ok {
		ctx.String(http.StatusUnauthorized, "User not authenticated")
		return
	}

	request.UserID = int(userId.(float64))
	request.SpecificID = int(specificId.(float64))
	request.Role = userRole.(string)

	// 4. Enhance context with user data (optional)
	reqCtx := ctx.Request.Context()

	err := cc.chatService.StreamChatResponseV2(reqCtx, request, w)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Println("Client disconnected during streaming")
			return
		}
		log.Printf("Service error: %v", err)
		ctx.String(http.StatusInternalServerError, "Failed to stream response: %v", err)
		return
	}

	log.Println("Request handled successfully")
}

func (cc *ChatController) RegisterRoutes(router *gin.Engine, jwtService *auth.ServiceImpl) {
	router.POST("/api/v1/chat/completions", middleware.Authenticate(jwtService), cc.ChatStreamHandler)
}
