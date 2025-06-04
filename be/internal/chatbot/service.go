package chatbot

import (
	"HNLP/be/internal/chatmanagement"
	"HNLP/be/internal/db"
	"HNLP/be/internal/llm"
	"HNLP/be/internal/search"
	"context"
	"encoding/json"
	"fmt"
	"github.com/invopop/jsonschema"
	"github.com/sashabaranov/go-openai"
	"io"
	"log"
	"net/http"
)

const (
	ToolPromptTemplate = `You are a chatbot for the university database. 
Your task is base on given tool, answer user query.
You should prioritize the function call that is most relevant to the user query.
In case you don't find any relevant function, you generate a Postgres SQL to use executeQuery function to run SQL query.
You should not use any other function to retrieve data, try to avoid use LIKE operator in SQL query, but if user query is too vague, you can use LIKE operator to get the data.
Here is the database schema: %s

Note that we have 3 concept about course, Lớp học phần is course_class, Lịch học is course_class_schedule, Môn học là course.
When you want to select course_class_schedule of a semester, you should join it with course_class to leverage course_class.semester_id
User ID is %d (This will be id of student or professor), User role is %s.
Note that current semester is 2024-2025-1 and semester have format like yyyy-yy-x where x is 1,2. And you should only use it to filter if user request.
When user specify a specific name, you should use it to filter the data.
Here is the user query: %s
In case there is not data return, you should inform user that there is no data or user does not have permission to access the data.
`
)

// IChatService defines the interface for chat services.
type IChatService interface {
}

// ChatService implements the IChatService interface.
type ChatService struct {
	aiProvider     llm.AIProvider
	db             db.HDb
	searchSrv      search.Service
	funcRegistry   llm.FuncRegistry
	chatManagement chatmanagement.Service
}

// NewChatService creates a new instance of ChatService.
func NewChatService(aiProvider llm.AIProvider, db db.HDb, searchSrv search.Service, funcRegistry llm.FuncRegistry) *ChatService {
	service := ChatService{
		aiProvider:   aiProvider,
		db:           db,
		searchSrv:    searchSrv,
		funcRegistry: funcRegistry,
	}
	return &service
}

func (cs *ChatService) StreamChatResponseV2(ctx context.Context, req ChatRequest, w io.Writer) error {
	// Step 1: Validate the user query base on the user role
	//validateResult, err := cs.validateUserQuery(ctx, req.Messages[len(req.Messages)-1].Content, userId, role)
	//if err != nil {
	//	log.Printf("Failed to validate user query: %v", err)
	//}
	//if !validateResult.IsValid {
	//	err := writeSSEResponse(w, StreamResponse{
	//		Choices: []Choice{{Delta: Delta{Content: validateResult.MessageRequest}}},
	//	})
	//	if err != nil {
	//		return err
	//	}
	//}

	ctx = context.WithValue(ctx, "userId", req.UserID)
	ctx = context.WithValue(ctx, "specificId", req.SpecificID)
	ctx = context.WithValue(ctx, "userRole", req.Role)

	// Step 2: Prepare LLM messages
	dbDDL, err := cs.db.LoadDDL()
	toolPrompt := fmt.Sprintf(ToolPromptTemplate, dbDDL, req.SpecificID, req.Role, req.Messages[len(req.Messages)-1].Content)
	funcDefs := cs.funcRegistry.GetFuncDefinitions()

	toolResponse, err := cs.getToolCallsByAI(ctx, toolPrompt, funcDefs)
	if err != nil {
		log.Printf("Failed to get tool calls: %v", err)
		return err
	}

	toolResults := make(map[string]string)
	for _, toolCall := range toolResponse.ToolCalls {
		executedResult, err := cs.funcRegistry.Execute(ctx, toolCall)
		if err != nil {
			log.Printf("Failed to execute tool call: %v", err)

			resp := StreamResponse{
				Choices: []Choice{{Delta: Delta{Content: err.Error()}}},
			}

			if err := writeSSEResponse(w, resp); err != nil {
				return err
			}
			fmt.Fprintf(w, "data: [DONE]\n\n")
			return nil

		}

		toolResults[toolCall.ID] = executedResult
	}

	// Step 4: Recall the AI provider to get the final answer
	naturalLangRequest := llm.CompletionRequest{
		Messages: []llm.Message{
			{
				Role:    openai.ChatMessageRoleUser,
				Content: toolPrompt,
			},
			{
				Role:       openai.ChatMessageRoleAssistant,
				Content:    toolResponse.Content,
				ToolCalls:  toolResponse.ToolCalls,
				ToolCallId: toolResponse.ToolCallId,
				Id:         toolResponse.Id,
			},
		},
		Model: openai.GPT4oMini20240718,
	}

	for toolId, result := range toolResults {
		naturalLangRequest.Messages = append(naturalLangRequest.Messages, llm.Message{
			Role:       openai.ChatMessageRoleTool,
			Content:    result,
			ToolCallId: toolId,
		})
	}

	// Step 5: Stream the response
	chunks, err := cs.aiProvider.StreamComplete(ctx, naturalLangRequest)
	if err != nil {
		log.Printf("Failed to stream complete results: %v", err)
		return err
	}

	// Accumulate the complete bot response
	//var fullContent strings.Builder

	for chunk := range chunks {
		if chunk.Done {
			// Save the complete bot message to the conversation
			fmt.Fprintf(w, "data: [DONE]\n\n")
			return nil
		}

		//// Accumulate content for saving later
		//fullContent.WriteString(chunk.Content)

		// Format SSE response
		resp := StreamResponse{
			Choices: []Choice{{Delta: Delta{Content: chunk.Content}}},
		}

		if err := writeSSEResponse(w, resp); err != nil {
			return err
		}
	}

	return nil
}

func (cs *ChatService) getToolCallsByAI(ctx context.Context, toolPrompt string, funcDefs []llm.FuncDefinition) (llm.Message, error) {
	toolRequest := llm.CompletionRequest{
		Messages: []llm.Message{
			{
				Role:    openai.ChatMessageRoleUser,
				Content: toolPrompt,
			},
		},
		Model:               openai.O3Mini20250131,
		Tools:               make([]llm.Tool, 0),
		FunctionCallingMode: llm.Required,
	}

	// Convert function definitions to tools
	for _, funcDef := range funcDefs {
		toolRequest.Tools = append(toolRequest.Tools, llm.Tool{
			Type:     llm.ToolTypeFunction,
			Function: &funcDef,
		})
	}

	// Step 3: Get tool call
	toolResponse, err := cs.aiProvider.Complete(ctx, toolRequest)
	return toolResponse, err
}

// ValidationResult holds the outcome of query validation
type ValidationResult struct {
	IsValid bool   `json:"is_valid" jsonschema:"description=Indicates if the query is valid based on user role"`
	Message string `json:"message" jsonschema:"description=Detailed message about the validation result"`
}

func (cs *ChatService) validateUserQuery(ctx context.Context, query string, userId int, role string) (ValidationResult, error) {
	prompt := fmt.Sprintf(`You are an agent that validates user queries for a university database. Here is the policy for each role:
These table, info are accessible for all roles: program, semester, course, course infomation, course_class, course_schedule, course_schedule_instructor. Other than these info, the user need to follow the policy below.
- Student role: Can only access their own personal info, course, administrative class and corresponding advisor, grades and above public data
- Professor role: Can only access their own personal info, courses they teach, corresponding students and grades, schedules and above public data
- Admin role: Can access all data.`)
	prompt += fmt.Sprintf("User role: %s, User ID: %d, User query: %s", role, userId, query)

	reflector := jsonschema.Reflector{
		DoNotReference: true,
		ExpandedStruct: false,
	}

	validationRequest := llm.CompletionRequest{
		Messages: []llm.Message{
			{
				Content: prompt,
				Role:    openai.ChatMessageRoleUser,
			},
		},
		ResponseFormat: &llm.ResponseFormat{
			Type:   llm.ResponseFormatTypeJson,
			Schema: reflector.Reflect(&ValidationResult{}),
			Name:   "ValidationResult",
		},
	}

	response, err := cs.aiProvider.Complete(ctx, validationRequest)
	if err != nil {
		return ValidationResult{}, fmt.Errorf("failed to validate user query: %v", err)
	}

	var result ValidationResult
	if err := json.Unmarshal([]byte(response.Content), &result); err != nil {
		return ValidationResult{}, fmt.Errorf("failed to parse validation response: %v", err)
	}

	return result, nil
}

//------------------Private helper functions------------------

func convertQueryResultToJSONString(queryResult *db.QueryResult) (string, error) {
	jsonBytes, err := json.MarshalIndent(queryResult, "", "  ") // Use MarshalIndent for pretty JSON
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

func writeSSEResponse(w io.Writer, resp StreamResponse) error {
	// Marshal the response to JSON
	jsonData, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("failed to marshal SSE response: %w", err)
	}

	// Write the SSE formatted message
	if _, err := fmt.Fprintf(w, "data: %s\n\n", jsonData); err != nil {
		return fmt.Errorf("failed to write SSE message: %w", err)
	}

	// If the writer supports flushing (like http.ResponseWriter), flush it
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	return nil
}
