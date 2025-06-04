package db

import (
	"context"
	"database/sql"
	"fmt"
	pgquery "github.com/pganalyze/pg_query_go/v6"
	"regexp"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

var ddlLoaders = make(map[string]DDLLoader)

// HDb defines the database operations interface
type HDb interface {
	LoadDDL() (string, error)
	ExecuteQuery(ctx context.Context, query QueryRequest) (*QueryResult, error)
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	//QueryRowContext(ctx context.Context, s string, id int, title string)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
}

// SQLHDb implements the HDb interface
type SQLHDb struct {
	*sqlx.DB
	DDLLoader
	authSer AuthorizationService
}

// NewHDb creates a new HDb instance
func NewHDb(driverName, dataSourceUrl string, authSer AuthorizationService) (*SQLHDb, error) {
	db, err := sqlx.Connect(driverName, dataSourceUrl)
	if err != nil {
		return nil, err
	}
	ddlLoader := ddlLoaders[driverName]

	return &SQLHDb{db, ddlLoader, authSer}, nil
}

// LoadDDL loads the database schema
func (db *SQLHDb) LoadDDL() (string, error) {
	return db.DDLLoader.LoadDDL(db.DB)
}

// ExecuteQuery executes a SQL query and returns the results
func (db *SQLHDb) ExecuteQuery(ctx context.Context, req QueryRequest) (*QueryResult, error) {
	req.Query = CleanSQL(req.Query)

	// Validate the query
	isValid, errMsg, err := db.validateQuery(ctx, req.Query)
	if err != nil {
		return nil, fmt.Errorf("error validating query: %v", err)
	}
	if !isValid {
		return nil, fmt.Errorf("%s", errMsg)
	}

	rows, err := db.QueryxContext(ctx, req.Query)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %v", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting columns: %v", err)
	}

	// Prepare result structure
	result := &QueryResult{}
	result.Metadata.Columns = columns

	// Scan rows into slice of maps
	var allRows []map[string]interface{}
	for rows.Next() {
		row := make(map[string]interface{})
		err := rows.MapScan(row)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}

		// Process each value in the row
		for key, val := range row {
			// Handle null values
			if val == nil {
				continue
			}

			// Convert to appropriate type
			switch v := val.(type) {
			case []byte:
				// Try to convert []byte to string
				row[key] = string(v)
			case time.Time:
				// Format time as ISO8601
				row[key] = v.Format(time.RFC3339)
			default:
				// Use value as is
				row[key] = v
			}
		}

		allRows = append(allRows, row)
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	result.Data = allRows
	result.Metadata.RowCount = len(allRows)

	return result, nil
}

// CleanSQL sanitizes SQL queries by removing problematic characters and normalizing whitespace
func CleanSQL(sql string) string {
	// Replace all newlines with spaces
	sql = strings.ReplaceAll(sql, "\n", " ")

	// Remove any excess whitespace
	sql = strings.TrimSpace(sql)

	// Optional: collapse multiple spaces into single space
	spacesRegex := regexp.MustCompile(`\s+`)
	sql = spacesRegex.ReplaceAllString(sql, " ")

	return sql
}

func (db *SQLHDb) validateQuery(ctx context.Context, query string) (bool, string, error) {
	// Use pg_query_go to parse the SQL query
	tree, err := pgquery.Parse(query)
	if err != nil {
		return false, "Failed to parse SQL query", err
	}

	// Only allow SELECT statements for security
	if len(tree.Stmts) == 0 {
		return false, "Empty query", nil
	}

	stmt := tree.Stmts[0]
	selectStmt := stmt.Stmt.GetSelectStmt()
	if selectStmt == nil {
		return false, "Only SELECT queries are allowed", nil
	}

	role := ctx.Value("userRole").(string)
	specificId := ctx.Value("specificId").(int)
	if role == "" {
		return false, "Role not found", nil
	}
	if specificId == 0 {
		return false, "User ID not found", nil
	}
	var userInfo UserContext

	switch role {
	case "student":
		userInfo, err = db.FetchStudentInfo(ctx, specificId)
		if err != nil {
			return false, "Failed to fetch student info", err
		}
	case "professor":
		userInfo, err = db.FetchProfessorInfo(ctx, specificId)
		if err != nil {
			return false, "Failed to fetch professor info", err
		}
	case "admin":
		userInfo, err = db.FetchAdminInfo(ctx, specificId)
		if err != nil {
			return false, "Failed to fetch admin info", err
		}
	default:
		return false, fmt.Sprintf("Unknown role: %s", role), nil
	}

	// Now use userInfo for authorization
	authResult, err := db.authSer.AuthorizeNode(stmt.GetStmt(), nil, false, userInfo)
	if err != nil {
		return false, "Xin lỗi bạn không có quyền truy cập vào dữ liệu này", err
	}

	return authResult.Authorized, "Xin lỗi bạn không có quyền truy cập vào dữ liệu này", nil
}

// Helper functions
func isPublicTable(tableName string) bool {
	publicTables := []string{
		"faculty",
		"program",
		"course",
		"course_program",
		"course_class",
		"course_class_schedule",
	}

	for _, table := range publicTables {
		if table == tableName {
			return true
		}
	}
	return false
}

type TableInfo struct {
	Name         string
	Alias        string            // Empty if no alias
	ColumnsAlias map[string]string // Map of alias to column name
	HasStar      bool              // Should default to true
}
