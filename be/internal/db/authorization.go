package db

import (
	"context"
	"errors"
	"fmt"
	om "github.com/elliotchance/orderedmap/v3"
	pgquery "github.com/pganalyze/pg_query_go/v6"
	"log"
	"strings"
)

type AuthorizationService interface {
	AuthorizeNode(node *pgquery.Node, tables *om.OrderedMap[string, *TableInfoV2], neg bool, userInfo UserContext) (*AuthorizationResult, error)
}

// AuthorizationServiceImpl handles SQL query authorization
type AuthorizationServiceImpl struct {
	schemaService SchemaService
}

// NewAuthorizationServiceImpl creates a new authorization service
func NewAuthorizationServiceImpl(service SchemaService) *AuthorizationServiceImpl {
	return &AuthorizationServiceImpl{
		schemaService: service,
	}
}

type AuthorizationResult struct {
	Authorized bool

	// Tables is a map of alias to array of TableInfo. Note that we should maintain the order of the tables
	Tables *om.OrderedMap[string, *TableInfoV2]

	TargetList []*pgquery.Node
}

type AuthorizationContext struct {
	Conditions       *om.OrderedMap[string, []int]
	AuthorizeColumns []string
	Bypass           bool
}

type AuthorizationCondition struct {
	AuthorizeColumn string
	ExpectedValues  []int // Changed to plural for clarity
}

type UserContext interface {
	GetRole() string
	GetID() int
}

// AuthorizeNode checks the authorization for a specific node in the SQL query.
// It validates whether the user with the given role and ID has access to the tables involved.
//
// Parameters:
// - node: the node to check
// - tables: additional tables to check authorization against
// - neg: a boolean indicating if we are in a negation context, in other words, the selected data doesn't contain any authorized data
// - role: the role of the user
// - userId: the ID of the user
//
// Returns:
// - bool: true if the node is authorized, false otherwise
// - []TableInfo: the list of remaining unauthorized tables
// - error: an error if any occurred during the authorization process
func (s *AuthorizationServiceImpl) AuthorizeNode(node *pgquery.Node, tables *om.OrderedMap[string, *TableInfoV2], neg bool, userInfo UserContext) (*AuthorizationResult, error) {
	if node.GetNode() == nil {
		return nil, errors.New("node is nil")
	}
	// We will ignore the node related to function call, group by, order by, limit, offset, target list etc. and return default false for it
	// Also ignore JSON, XML, and other data types because our database is not using them
	// Also consider PARTITION, WINDOW as unauthorized so that they need to be authorized by a WHERE clause
	// Also only consider SELECT queries and not modifying queries like INSERT, UPDATE, DELETE,etc
	switch node.GetNode().(type) {
	case *pgquery.Node_SelectStmt:
		return s.authorizeSelectStmt(node.GetSelectStmt(), tables, neg, userInfo)
	case *pgquery.Node_BoolExpr:
		return s.authorizeBoolExpr(node.GetBoolExpr(), tables, neg, userInfo)
	case *pgquery.Node_AExpr:
		return s.authorizeAExpr(node.GetAExpr(), tables, neg, userInfo)
	case *pgquery.Node_SubLink:
		return s.authorizeSubLink(node.GetSubLink(), tables, neg, userInfo)
	case *pgquery.Node_JoinExpr:
		return s.authorizeJoinExpr(node.GetJoinExpr(), tables, neg, userInfo)
	case *pgquery.Node_FromExpr:
		return s.authorizeFromExpr(node.GetFromExpr(), tables, neg, userInfo)
	case *pgquery.Node_RangeVar:
		return s.authorizeRangeVar(node.GetRangeVar(), tables, userInfo)
	case *pgquery.Node_RangeSubselect:
		return s.authorizeRangeSubselect(node.GetRangeSubselect(), tables, neg, userInfo)
	case *pgquery.Node_WithClause:
		return s.authorizeWithClause(node.GetWithClause(), tables, neg, userInfo)
	case *pgquery.Node_Query:
	case *pgquery.Node_ColumnRef:
	case *pgquery.Node_AIndirection:
	case *pgquery.Node_AIndices:
	case *pgquery.Node_AArrayExpr:
	case *pgquery.Node_CommonTableExpr:
	case *pgquery.Node_FetchStmt:
	case *pgquery.Node_PrepareStmt:
	case *pgquery.Node_ExecuteStmt:

	}

	return &AuthorizationResult{
		Authorized: false,
		Tables:     tables,
	}, nil
}

func (s *AuthorizationServiceImpl) authorizeSelectStmt(stmt *pgquery.SelectStmt, tables *om.OrderedMap[string, *TableInfoV2], neg bool, userInfo UserContext) (*AuthorizationResult, error) {
	result := &AuthorizationResult{
		Authorized: true,
		Tables:     om.NewOrderedMap[string, *TableInfoV2](),
	}

	if stmt.GetIntoClause() != nil {
		return result, nil
	}

	if stmt.GetWithClause() != nil {
		for _, cte := range stmt.GetWithClause().GetCtes() {
			cteResult, err := s.AuthorizeNode(cte, tables, neg, userInfo)
			if err != nil {
				return nil, err
			}
			result.Tables = s.MergeMaps(cteResult.Tables, result.Tables)
			result.Authorized = result.Authorized && cteResult.Authorized
		}
	}

	if stmt.GetOp() == pgquery.SetOperation_SETOP_UNION {
		// Similar to the OR condition, the remaining unauthorized tables will be union
		// of all unauthorized tables for left and right select
		lResult, err := s.authorizeSelectStmt(stmt.GetLarg(), tables, neg, userInfo)
		if err != nil {
			return nil, err
		}
		rResult, err := s.authorizeSelectStmt(stmt.GetRarg(), tables, neg, userInfo)
		if err != nil {
			return nil, err
		}
		result.Authorized = lResult.Authorized && rResult.Authorized
		// This is a little trick, we only merge the right to left. But because the scope of lResult and rResult is only here
		// So do this to have a little bit better performance and convenience in code
		s.UpdateUnauthorizedTablesByUnion(result.Tables, []*om.OrderedMap[string, *TableInfoV2]{s.MergeMaps(lResult.Tables, rResult.Tables)})
		if lResult.TargetList != nil && rResult.TargetList != nil {
			// The target list should be the same for both sides
			result.TargetList = lResult.TargetList
		}
		return result, nil
	}

	if stmt.GetOp() == pgquery.SetOperation_SETOP_INTERSECT {
		// Similar to the AND condition, the remaining unauthorized tables will be intersection
		lResult, err := s.authorizeSelectStmt(stmt.GetLarg(), tables, neg, userInfo)
		if err != nil {
			return nil, err
		}
		rResult, err := s.authorizeSelectStmt(stmt.GetRarg(), tables, neg, userInfo)
		if err != nil {
			return nil, err
		}
		result.Authorized = lResult.Authorized || rResult.Authorized
		// Update result tables like above
		s.UpdateUnauthorizedTablesByIntersection(result.Tables, []*om.OrderedMap[string, *TableInfoV2]{s.MergeMaps(lResult.Tables, rResult.Tables)})
		if lResult.TargetList != nil && rResult.TargetList != nil {
			// The target list should be the same for both sides
			result.TargetList = lResult.TargetList
		}
		return result, nil
	}

	if stmt.GetOp() == pgquery.SetOperation_SETOP_EXCEPT {
		// We ignore the right SELECT can exclude all unauthorized data of the left Select and make the whole query authorized
		// Because it can be rewritten in as an additional condition in the left select
		// It also is complex case, if this happens, it is usually intended of the hacker
		return s.authorizeSelectStmt(stmt.GetLarg(), tables, neg, userInfo)
	}

	for _, fromItem := range stmt.GetFromClause() {
		fromResult, err := s.AuthorizeNode(fromItem, tables, neg, userInfo)
		if err != nil {
			return nil, err
		}
		result.Tables = s.MergeMaps(result.Tables, fromResult.Tables)
		result.Authorized = result.Authorized && fromResult.Authorized
	}

	// We are certainly that the where clause and having clause don't discover needed new tables.
	// If they discovered new tables, it should be handled inside its own node
	if !result.Authorized && stmt.GetWhereClause() != nil {
		whereResult, err := s.AuthorizeNode(stmt.WhereClause, result.Tables, neg, userInfo)
		if err != nil {
			return nil, err
		}
		// Reassign the tables to the result, because the where clause will filter out authorized tables
		result.Tables = whereResult.Tables
		result.Authorized = whereResult.Authorized
	}

	// The HAVING clause is applied after the WHERE clause
	if !result.Authorized && stmt.GetHavingClause() != nil {
		havingResult, err := s.AuthorizeNode(stmt.HavingClause, result.Tables, neg, userInfo)
		if err != nil {
			return nil, err
		}
		// Also update the result tables
		result.Tables = havingResult.Tables
		result.Authorized = havingResult.Authorized
	}

	if stmt.GetTargetList() != nil {
		// To do: Refactor code to also support authorize subquery in Select (subquery) from table
		result.TargetList = stmt.GetTargetList()
	}

	return result, nil
}

// authorizeFromExpr authorizes the FROM clause of a SQL statement.
func (s *AuthorizationServiceImpl) authorizeFromExpr(node *pgquery.FromExpr, tables *om.OrderedMap[string, *TableInfoV2], neg bool, userInfo UserContext) (*AuthorizationResult, error) {
	var combinedTables *om.OrderedMap[string, *TableInfoV2]
	for _, table := range node.GetFromlist() {
		tableResult, err := s.AuthorizeNode(table, tables, neg, userInfo)
		if err != nil {
			return nil, err
		}
		combinedTables = s.MergeMaps(combinedTables, tableResult.Tables)
	}

	return s.AuthorizeNode(node.GetQuals(), combinedTables, neg, userInfo)
}

func (s *AuthorizationServiceImpl) authorizeCommonTableExpr(expr *pgquery.Node_CommonTableExpr, tables []TableInfo, userInfo UserContext) (*AuthorizationResult, error) {
	return nil, nil
}

func (s *AuthorizationServiceImpl) authorizeJoinExpr(joinExpr *pgquery.JoinExpr, tables *om.OrderedMap[string, *TableInfoV2], neg bool, userInfo UserContext) (*AuthorizationResult, error) {
	if joinExpr == nil {
		return nil, fmt.Errorf("joinExpr is nil")
	}

	if joinExpr.GetLarg() == nil {
		return nil, fmt.Errorf("left side of join is nil")
	}
	if joinExpr.GetRarg() == nil {
		return nil, fmt.Errorf("right side of join is nil")
	}

	lResult, err := s.AuthorizeNode(joinExpr.GetLarg(), tables, neg, userInfo)
	if err != nil {
		return nil, err
	}

	rResult, err := s.AuthorizeNode(joinExpr.GetRarg(), tables, neg, userInfo)
	if err != nil {
		return nil, err
	}

	var curTables *om.OrderedMap[string, *TableInfoV2]
	curTables = s.MergeMaps(curTables, lResult.Tables)
	curTables = s.MergeMaps(curTables, rResult.Tables)

	// Both sides of the join are authorized
	if lResult.Authorized && rResult.Authorized {
		if joinAlias := joinExpr.GetAlias(); joinAlias != nil {
			curTables = s.createVirtualTableForAlias(joinAlias, nil, curTables)
		}
		return &AuthorizationResult{
			Authorized: true,
			Tables:     curTables,
		}, nil
	}

	// Process natural joins or joins with USING clause
	if joinExpr.GetUsingClause() != nil || joinExpr.GetIsNatural() {
		if joinAlias := joinExpr.GetAlias(); joinAlias != nil {
			curTables = s.createVirtualTableForAlias(joinAlias, nil, curTables)
		}
		if userInfo.GetRole() == "admin" {
			// TODO: Remove all unauthorized tables

			return &AuthorizationResult{
				Authorized: true,
				Tables:     curTables,
			}, nil
		}
		return &AuthorizationResult{
			Authorized: false,
			Tables:     curTables,
		}, nil
	}

	// Determine unauthorized tables based on join type
	switch joinExpr.GetJointype() {
	case pgquery.JoinType_JOIN_INNER, pgquery.JoinType_JOIN_UNIQUE_INNER:
		// We only need to check the unauthorized tables that belong to both sides
		// In other words, we ignore the unauthorized tables that don't exist in both sides
		s.UpdateUnauthorizedTablesByIntersection(curTables, []*om.OrderedMap[string, *TableInfoV2]{lResult.Tables, rResult.Tables})

	case pgquery.JoinType_JOIN_LEFT:
		// We ignore the right side of the join
		for _, table := range rResult.Tables.AllFromFront() {
			table.UnAuthorizedTables = om.NewOrderedMap[string, *TableInfoV2]()
		}
	case pgquery.JoinType_JOIN_RIGHT:
		// We ignore the left side of the join
		for _, table := range lResult.Tables.AllFromFront() {
			table.UnAuthorizedTables = om.NewOrderedMap[string, *TableInfoV2]()
		}
	case pgquery.JoinType_JOIN_FULL, pgquery.JoinType_JOIN_UNIQUE_OUTER:
		// We need to check all unauthorized tables from both sides, so curTables is good
		// and we will not change anything in the tables
	default:
		return nil, fmt.Errorf("unsupported join type: %v", joinExpr.GetJointype())
	}

	qualResult, err := s.AuthorizeNode(joinExpr.GetQuals(), curTables, neg, userInfo)
	if err != nil {
		return nil, err
	}

	if joinAlias := joinExpr.GetAlias(); joinAlias != nil {
		// Create and assign the virtual table
		qualResult.Tables = s.createVirtualTableForAlias(joinAlias, nil, qualResult.Tables)
	}

	return qualResult, nil
}

// authorizeSubLink authorizes a SubLink node in the SQL query.
func (s *AuthorizationServiceImpl) authorizeSubLink(link *pgquery.SubLink, tables *om.OrderedMap[string, *TableInfoV2], neg bool, userInfo UserContext) (*AuthorizationResult, error) {
	// This node only parse fully after analysis phase, so we only can to authorize subquery here
	return s.AuthorizeNode(link.GetSubselect(), tables, neg, userInfo)
}

func (s *AuthorizationServiceImpl) authorizeRangeSubselect(node *pgquery.RangeSubselect, tables *om.OrderedMap[string, *TableInfoV2], neg bool, userInfo UserContext) (*AuthorizationResult, error) {
	if node.GetLateral() {
		// TODO: Recheck
		return nil, fmt.Errorf("lateral is not supported")
	}

	authResult, err := s.AuthorizeNode(node.GetSubquery(), tables, false, userInfo)
	if err != nil {
		return nil, err
	}

	if node.GetAlias() != nil {
		authResult.Tables = s.createVirtualTableForAlias(node.GetAlias(), authResult.TargetList, authResult.Tables)
		authResult.TargetList = nil
	}

	return authResult, nil
}

// authorizeRangeVar get the table name (with authorization information) from the range var
// We should not pass tables to this function because we don't check the authorization of extra tables here
// But keep the tables parameter to align with the other functions
func (s *AuthorizationServiceImpl) authorizeRangeVar(node *pgquery.RangeVar, tables *om.OrderedMap[string, *TableInfoV2], userInfo UserContext) (*AuthorizationResult, error) {
	if node == nil {
		return nil, fmt.Errorf("range var is nil")
	}

	table := &TableInfoV2{
		Name:               node.GetRelname(),
		Columns:            om.NewOrderedMap[string, *ColumnInfo](), // Need to check with select statement to decide should we add columns here or when we have target list
		Alias:              node.GetRelname(),
		Authorized:         isPublicTable(node.GetRelname()) || userInfo.GetRole() == "admin",
		UnAuthorizedTables: om.NewOrderedMap[string, *TableInfoV2](),
		IsDatabase:         true,
	}

	// Because we can have col names alias for join expr, etc. which doesn't need to reach TargetList
	// So we populate all columns right here
	for _, colName := range s.schemaService.GetColumns(table.Name) {
		table.Columns.Set(colName, &ColumnInfo{
			Name:        colName,
			SourceTable: table,
		})
	}

	if node.GetAlias() != nil {
		table.Alias = node.GetAlias().GetAliasname()
	}

	tables = s.MergeMaps(tables, om.NewOrderedMapWithElements(&om.Element[string, *TableInfoV2]{Key: table.Alias, Value: table}))

	return &AuthorizationResult{
		Authorized: isPublicTable(table.Name) || userInfo.GetRole() == "admin",
		Tables:     tables,
	}, nil
}

// authorizeWithClause authorizes a WITH clause in the SQL query.
// In most cases, the tables parameter is empty because we don't need to check authorization of extra tables
// But we need to keep it to align with the other functions
func (s *AuthorizationServiceImpl) authorizeWithClause(clause *pgquery.WithClause, tables *om.OrderedMap[string, *TableInfoV2], neg bool, userInfo UserContext) (*AuthorizationResult, error) {
	result := &AuthorizationResult{
		Authorized: true,
		Tables:     om.NewOrderedMap[string, *TableInfoV2](),
	}
	for _, cte := range clause.GetCtes() {
		cteResult, err := s.AuthorizeNode(cte, tables, neg, userInfo)
		if err != nil {
			return nil, err
		}
		result.Tables = s.MergeMaps(cteResult.Tables, result.Tables)
		result.Authorized = result.Authorized && cteResult.Authorized
	}
	return result, nil
}

// authorizeBoolExpr authorizes a BoolExpr node in the SQL query (mostly used in WHERE and ON clause).
func (s *AuthorizationServiceImpl) authorizeBoolExpr(boolExpr *pgquery.BoolExpr, tables *om.OrderedMap[string, *TableInfoV2], neg bool, userInfo UserContext) (*AuthorizationResult, error) {
	if boolExpr == nil {
		return &AuthorizationResult{
			Authorized: tables.Len() == 0,
			Tables:     tables,
		}, nil
	}

	result := &AuthorizationResult{
		Authorized: true,
		Tables:     CloneUnAuthorizedTables(tables),
	}

	switch boolExpr.GetBoolop() {
	case pgquery.BoolExprType_OR_EXPR:
		// All subexpressions must be authorized, so the remaining unauthorized tables will be union
		// of all unauthorized tables for each subexpression
		var authResultTables []*om.OrderedMap[string, *TableInfoV2]
		for _, arg := range boolExpr.Args {
			authResult, err := s.AuthorizeNode(arg, result.Tables, neg, userInfo)
			if err != nil {
				return nil, err
			}
			authResultTables = append(authResultTables, authResult.Tables)
		}
		s.UpdateUnauthorizedTablesByUnion(result.Tables, authResultTables)

	case pgquery.BoolExprType_AND_EXPR:
		// If intersection of unauthorized tables of all subexpressions is empty, then the whole expression is authorized
		var authResultTables []*om.OrderedMap[string, *TableInfoV2]
		for _, arg := range boolExpr.Args {
			authResult, err := s.AuthorizeNode(arg, result.Tables, neg, userInfo)
			if err != nil {
				return nil, err
			}
			authResultTables = append(authResultTables, authResult.Tables)
		}
		s.UpdateUnauthorizedTablesByIntersection(result.Tables, authResultTables)

	case pgquery.BoolExprType_NOT_EXPR:
		// Negation: check if the subexpression is unauthorized,
		// The tables only authorized if contains something like user_id != id
		if len(boolExpr.Args) != 1 {
			return nil, fmt.Errorf("NOT expression must have exactly one argument")
		}
		authResult, err := s.AuthorizeNode(boolExpr.Args[0], result.Tables, !neg, userInfo)
		if err != nil {
			return nil, err
		}
		result.Authorized = authResult.Authorized
		result.Tables = authResult.Tables
	}

	for _, table := range result.Tables.AllFromFront() {
		if table.IsDatabase {
			result.Authorized = result.Authorized && table.Authorized
		} else {
			result.Authorized = result.Authorized && table.UnAuthorizedTables.Len() == 0
		}
	}

	return result, nil
}

// authorizeAExpr authorizes an A_Expr node in the SQL query (mostly used in WHERE clause).
// tables is the list of tables to check authorization against.
// We shouldn't modify the tables parameter because in outer condition (like OR) we need to pass same tables for all member of the condition
func (s *AuthorizationServiceImpl) authorizeAExpr(expr *pgquery.A_Expr, tables *om.OrderedMap[string, *TableInfoV2], neg bool, userInfo UserContext) (*AuthorizationResult, error) {
	if expr == nil {
		return nil, errors.New("AExpr is nil")
	}

	resultTables := CloneUnAuthorizedTables(tables)

	if expr.GetLexpr() == nil || expr.GetRexpr() == nil {
		return &AuthorizationResult{
			Authorized: false,
			Tables:     resultTables,
		}, nil
	}

	wantOperator := "="
	if neg {
		wantOperator = "<>"
	}
	if s.getStringValueFromNode(expr.GetName()[0]) != wantOperator {
		return &AuthorizationResult{
			Authorized: false,
			Tables:     resultTables,
		}, nil
	}

	switch expr.GetKind() {
	case pgquery.A_Expr_Kind_AEXPR_OP:
		// Case: a op b (with op is in https://www.postgresql.org/docs/6.3/c09.htm)
		// Base case: We only allow column = id or id = column
		// Other case like subquery = column, etc don't contribute much to the authorization of the main query
		columnRef, aConst := s.extractColumnRefAndConst(expr)

		if columnRef != nil && aConst != nil {
			// Loop through need to check tables to see which tables this expression can authorize
			for _, table := range resultTables.AllFromFront() {
				if table.IsDatabase {
					if table.Authorized {
						// If the table is authorized, we don't need to check it
						continue
					}
					if s.isAuthorizedExpression(columnRef, aConst, table, table, userInfo) {
						table.Authorized = true
					}
				} else {
					var tablesToRemove []string
					for unAuthAlias, unauthorizedTable := range table.UnAuthorizedTables.AllFromFront() {
						if s.isAuthorizedExpression(columnRef, aConst, table, unauthorizedTable, userInfo) {
							// Just too much careful, we also set the table to authorized
							authorizedTable, _ := table.UnAuthorizedTables.Get(unAuthAlias)
							authorizedTable.Authorized = true
							// Remove the table from the list of unauthorized tables
							tablesToRemove = append(tablesToRemove, unAuthAlias)
						}
					}
					for _, tableToRemove := range tablesToRemove {
						// Remove the table from the list of unauthorized tables
						table.UnAuthorizedTables.Delete(tableToRemove)
					}
					if table.UnAuthorizedTables.Len() == 0 {
						table.Authorized = true
					}
				}
			}
		}

		// Case: RowExpr = SubLink
		// We will support (t.user_id, t.col1) = (SELECT ... where user_id = id)
		if expr.GetLexpr() != nil && expr.GetLexpr().GetRowExpr() != nil {
			for _, arg := range expr.GetLexpr().GetRowExpr().GetArgs() {
				if arg.GetColumnRef() == nil {
					continue
				}
				fields := arg.GetColumnRef().GetFields()
				if len(fields) <= 1 || fields[0].GetString_() == nil || fields[1].GetString_() == nil {
					continue
				}
				tableElement := resultTables.GetElement(fields[0].GetString_().GetSval())
				if tableElement == nil {
					continue
				}
				// Need to recheck this logic: when this is virtual table, we can't check naively like this
				if tableElement.Value.Authorized {
					// If the table is authorized, we don't need to check it
					continue
				}

				if tableElement.Value.IsDatabase {
					authContext := s.getAuthorizationColumn(tableElement.Value.Name, userInfo)
					if ContainsString(authContext.AuthorizeColumns, tableElement.Value.getRealColName(fields[1].GetString_().GetSval())) {
						// We don't have the authorization column, so we can't authorize this table
						continue
					}

					authResult, _ := s.AuthorizeNode(expr.GetRexpr(), om.NewOrderedMapWithElements(tableElement), neg, userInfo)
					if authResult.Authorized {
						tableElement.Value.Authorized = true
					}
				} else {
					resolvedCol := tableElement.Value.getRealColumnInfoFromAlias(fields[1].GetString_().GetSval())
					if resolvedCol == nil {
						continue
					}
					authResult, _ := s.AuthorizeNode(expr.GetRexpr(), nil, neg, userInfo)
					for _, table := range authResult.Tables.AllFromFront() {
						if !table.IsDatabase {
							// Don't support yet, to support in the future we need to have an extra field in TableInfoV2 to quick access all discovered tables
							continue
						}
						if table.Name == resolvedCol.SourceTable.Name && table.Authorized {
							// Too much careful
							resolvedCol.SourceTable.Authorized = true
							tableElement.Value.UnAuthorizedTables.Delete(table.Alias) // Can avoid table.Alias when support quick access field above
							if tableElement.Value.UnAuthorizedTables.Len() == 0 {
								tableElement.Value.Authorized = true
							}
							break
						}
					}

				}
			}
		}
	case pgquery.A_Expr_Kind_AEXPR_IN:
		// Case: a IN (b, c, d)
		// We only allow authorizeColumn IN (id) or id IN (column)
		if expr.GetLexpr().GetColumnRef() == nil || expr.GetRexpr().GetList() == nil || len(expr.GetRexpr().GetList().GetItems()) != 1 {
			break
		}
		columnRef := expr.GetLexpr().GetColumnRef()
		itemList := expr.GetRexpr().GetList().GetItems()

		for _, table := range resultTables.AllFromFront() {
			if table.IsDatabase {
				if table.Authorized {
					// If the table is authorized, we don't need to check it
					continue
				}
				if s.isAuthorizedExpression(columnRef, itemList[0].GetAConst(), table, table, userInfo) {
					table.Authorized = true
				}
			} else {
				var tablesToRemove []string
				for unAuthAlias, unauthorizedTable := range table.UnAuthorizedTables.AllFromFront() {
					if s.isAuthorizedExpression(columnRef, itemList[0].GetAConst(), table, unauthorizedTable, userInfo) {
						// Just too much careful, we also set the table to authorized
						authorizedTable, _ := table.UnAuthorizedTables.Get(unAuthAlias)
						authorizedTable.Authorized = true
						// Remove the table from the list of unauthorized tables
						tablesToRemove = append(tablesToRemove, unAuthAlias)

					}
				}

				for _, tableToRemove := range tablesToRemove {
					// Remove the table from the list of unauthorized tables
					table.UnAuthorizedTables.Delete(tableToRemove)
				}
				if table.UnAuthorizedTables.Len() == 0 {
					table.Authorized = true
				}
			}
		}
	}

	// Build the result
	authorized := true
	for _, table := range resultTables.AllFromFront() {
		if table.IsDatabase {
			authorized = authorized && table.Authorized
		} else {
			authorized = authorized && table.UnAuthorizedTables.Len() == 0
		}
	}

	return &AuthorizationResult{Authorized: authorized, Tables: resultTables}, nil
}

// createVirtualTableForAlias creates a virtual table for the given alias.
// It should keep reference to the original tables and columns.
// joinAlias is the alias for the join expression.
// targetList is the selected columns for new virtual table (optional).
// curTables is the current tables in the query.
func (s *AuthorizationServiceImpl) createVirtualTableForAlias(alias *pgquery.Alias, targetList []*pgquery.Node, curTables *om.OrderedMap[string, *TableInfoV2]) *om.OrderedMap[string, *TableInfoV2] {
	// We have an alias for a subscope, so we will create a new wrapper TableInfo corresponding to the alias
	virtualTable := &TableInfoV2{
		Name:               alias.GetAliasname(),
		Columns:            om.NewOrderedMap[string, *ColumnInfo](),
		Alias:              alias.GetAliasname(),
		UnAuthorizedTables: om.NewOrderedMap[string, *TableInfoV2](),
	}

	// Convert the column aliases to a slice of strings
	var colAliases []string
	if alias.GetColnames() != nil {
		colAliases = make([]string, 0, len(alias.GetColnames()))
		for _, colName := range alias.GetColnames() {
			if colName.GetString_() != nil {
				colAliases = append(colAliases, colName.GetString_().GetSval())
			}
		}
	}

	allColCnt := 0
	for _, table := range curTables.AllFromFront() {
		allColCnt += table.Columns.Len()
	}

	curIndex := 0
	if targetList != nil {
		for _, colNode := range targetList {
			resTarget := colNode.GetResTarget()
			if resTarget == nil || resTarget.GetVal() == nil || resTarget.GetVal().GetColumnRef() == nil {
				// We only care about column references now
				// Other node will be support in the future
				continue
			}
			fields := resTarget.GetVal().GetColumnRef().GetFields()
			if len(fields) == 0 {
				continue
			}

			if fields[0].GetAStar() != nil {
				// Case : SELECT * FROM ...
				// All tables should populate their columns at RangeVar
				// We only need collect them to the virtual table
				nextIndex := curIndex + allColCnt
				if nextIndex > len(colAliases) {
					nextIndex = len(colAliases)
				}
				virtualTable.createAllColumnsFromSourceTableList(curTables.Values(), colAliases[curIndex:nextIndex])
				curIndex = nextIndex

			} else if len(fields) == 2 && fields[1].GetAStar() != nil && fields[0].GetString_() != nil {
				// Case : SELECT table.* FROM ...
				selectedTableName := fields[0].GetString_().GetSval()
				selectedTable, ok := curTables.Get(selectedTableName)
				if !ok {
					continue // Skip nil tables, but this should not happen
				}
				nextIndex := curIndex + selectedTable.Columns.Len()
				if nextIndex > len(colAliases) {
					nextIndex = len(colAliases)
				}
				virtualTable.createAllColumnFromSourceTable(selectedTable, colAliases[curIndex:nextIndex])
				curIndex = nextIndex
			} else if fields[0].GetString_() != nil {
				// Case : SELECT table.col (AS alias) FROM ...
				selectedTableName := fields[0].GetString_().GetSval()
				selectedColName := fields[1].GetString_().GetSval()
				selectedTable, ok := curTables.Get(selectedTableName)
				if !ok {
					continue // Skip nil tables, but this should not happen
				}

				colAlias := selectedColName
				if resTarget.GetName() != "" {
					colAlias = resTarget.GetName()
				}
				if curIndex < len(colAliases) && colAliases[curIndex] != "" {
					colAlias = colAliases[curIndex]
					curIndex++
				}
				virtualTable.createColumnFromSourceTable(selectedColName, selectedTable, &colAlias)
			}
		}
	} else {
		// when targetList is nil, we create a virtual table like SELECT * FROM ... case
		virtualTable.createAllColumnsFromSourceTableList(curTables.Values(), colAliases)
	}
	// If there is unauthorized tables, we need to add them to the virtual table
	for tableAlias, table := range curTables.AllFromFront() {
		if !table.Authorized {
			virtualTable.UnAuthorizedTables.Set(tableAlias, table)
		}
	}

	// Also populate the unauthorized tables
	// In case all tables are authorized, we should not have any unauthorized tables
	for _, table := range curTables.AllFromFront() {
		if table.UnAuthorizedTables.Len() != 0 {
			for tableAlias, unauthorizedTable := range table.UnAuthorizedTables.AllFromFront() {
				virtualTable.UnAuthorizedTables.Set(tableAlias, unauthorizedTable)
			}
		}
	}

	// Add the virtual table to the tables map
	if virtualTable.UnAuthorizedTables.Len() == 0 {
		virtualTable.Authorized = true
	}

	return om.NewOrderedMapWithElements(&om.Element[string, *TableInfoV2]{Key: virtualTable.Alias, Value: virtualTable})
}

func (s *AuthorizationServiceImpl) getStringValueFromNode(node *pgquery.Node) string {
	if node == nil {
		return ""
	}

	switch n := node.GetNode().(type) {
	case *pgquery.Node_String_:
		return n.String_.Sval
	case *pgquery.Node_Integer:
		return fmt.Sprintf("%d", n.Integer.Ival)
	case *pgquery.Node_Float:
		return n.Float.Fval
	case *pgquery.Node_ColumnRef:
		// For column references, concatenate field names
		var parts []string
		for _, field := range n.ColumnRef.Fields {
			if str := field.GetString_(); str != nil {
				parts = append(parts, str.GetSval())
			}
		}
		return strings.Join(parts, ".")
	case *pgquery.Node_AConst:
		// Extract value from constant
		return ""
	}

	// Default case - return empty string for unsupported node types
	return ""
}

// extractColumnRefAndConst extracts the column reference and constant from an A_Expr node.
// It returns the column reference and constant as separate values and nil if not applicable.
func (s *AuthorizationServiceImpl) extractColumnRefAndConst(aExpr *pgquery.A_Expr) (*pgquery.ColumnRef, *pgquery.A_Const) {
	if aExpr.GetLexpr() == nil || aExpr.GetRexpr() == nil {
		return nil, nil
	}

	var columnRef *pgquery.ColumnRef
	var aConst *pgquery.A_Const

	// Check if column is on left and constant is on right
	if aExpr.GetLexpr().GetColumnRef() != nil && aExpr.GetRexpr().GetAConst() != nil {
		columnRef = aExpr.GetLexpr().GetColumnRef()
		aConst = aExpr.GetRexpr().GetAConst()
	} else if aExpr.GetLexpr().GetAConst() != nil && aExpr.GetRexpr().GetColumnRef() != nil {
		columnRef = aExpr.GetRexpr().GetColumnRef()
		aConst = aExpr.GetLexpr().GetAConst()
	}

	return columnRef, aConst
}

// UpdateUnauthorizedTablesByUnion updates the unauthorized tables in the destination list
func (s *AuthorizationServiceImpl) UpdateUnauthorizedTablesByUnion(dst *om.OrderedMap[string, *TableInfoV2], tablesList []*om.OrderedMap[string, *TableInfoV2]) {
	// All subexpressions must be authorized, so the remaining unauthorized tables will be union
	// of all unauthorized tables for each subexpression
	if tablesList == nil || len(tablesList) == 0 {
		return
	}

	// Map table alias to a set of unauthorized table aliases (in go it is a map)
	tableUnAuth := make(map[string]map[string]bool)
	for _, table := range dst.AllFromFront() {
		tableUnAuth[table.Alias] = make(map[string]bool)
	}

	for tableAlias, table := range dst.AllFromFront() {
		for _, tables := range tablesList {
			curTableEle := tables.GetElement(tableAlias)
			if curTableEle == nil {
				// This tablesList entry doesn't contain the table, this should never happen
				log.Printf("Warning: table %s not found in tablesList", tableAlias)
				continue
			}
			if table.IsDatabase {
				tableUnAuth[tableAlias][curTableEle.Key] = tableUnAuth[tableAlias][curTableEle.Key] || !curTableEle.Value.Authorized
			} else {
				for unAuthAlias, _ := range table.UnAuthorizedTables.AllFromFront() {
					curUnauthorizedTable, ok := curTableEle.Value.UnAuthorizedTables.Get(unAuthAlias)
					if !ok {
						// This tablesList entry doesn't contain the unauthorized table, this mean it is authorized
						continue
					}
					tableUnAuth[tableAlias][unAuthAlias] = tableUnAuth[tableAlias][unAuthAlias] || !curUnauthorizedTable.Authorized
				}
			}
		}
	}

	for _, table := range dst.AllFromFront() {
		if table.IsDatabase {
			table.Authorized = !tableUnAuth[table.Alias][table.Alias]
		} else {
			// Only remove if all corresponding tables in tableList are authorized
			// We assume that the dst already contains all unauthorized tables in tableList
			for unAuthAlias, unAuth := range tableUnAuth[table.Alias] {
				if !unAuth {
					// Remove the table from the list of unauthorized tables
					table.UnAuthorizedTables.Delete(unAuthAlias)
				}
			}
			if table.UnAuthorizedTables.Len() == 0 {
				table.Authorized = true
			}
		}
	}
}

// UpdateUnauthorizedTablesByIntersection updates the unauthorized tables in the destination list
// We only care about tableList data, but the dst should declare the boundaries of the tables (usually tables before transform to tablesList)
func (s *AuthorizationServiceImpl) UpdateUnauthorizedTablesByIntersection(dst *om.OrderedMap[string, *TableInfoV2], tablesList []*om.OrderedMap[string, *TableInfoV2]) {
	// If intersection of unauthorized tables of all subexpressions is empty, then the whole expression is authorized
	// Map table alias to a set of unauthorized table aliases (in go it is a map)
	if tablesList == nil || len(tablesList) == 0 {
		return
	}

	tableAuth := make(map[string]map[string]bool)
	for _, table := range dst.AllFromFront() {
		tableAuth[table.Alias] = make(map[string]bool)
	}

	for tableAlias, table := range dst.AllFromFront() {
		for _, tables := range tablesList {
			curTableEle := tables.GetElement(tableAlias)
			if curTableEle == nil {
				// This tablesList entry doesn't contain the table, this can happen in case (a join b) join c
				//
				log.Printf("Warning: table %s not found in tablesList", tableAlias)
				tableAuth[tableAlias][tableAlias] = true
				continue
			}
			if table.IsDatabase {
				tableAuth[tableAlias][curTableEle.Key] = tableAuth[tableAlias][curTableEle.Key] || curTableEle.Value.Authorized
			} else {
				for unAuthAlias, _ := range table.UnAuthorizedTables.AllFromFront() {
					curUnauthorizedTable, ok := curTableEle.Value.UnAuthorizedTables.Get(unAuthAlias)
					if !ok {
						// This tablesList entry doesn't contain the unauthorized table, this mean it is authorized
						tableAuth[tableAlias][unAuthAlias] = true
						continue
					}
					tableAuth[tableAlias][unAuthAlias] = tableAuth[tableAlias][unAuthAlias] || curUnauthorizedTable.Authorized
				}
			}
		}
	}

	for _, table := range dst.AllFromFront() {
		if table.IsDatabase {
			table.Authorized = tableAuth[table.Alias][table.Alias]
		} else {
			for authAlias, auth := range tableAuth[table.Alias] {
				if auth {
					// Remove the table from the list of unauthorized tables
					table.UnAuthorizedTables.Delete(authAlias)
				}
			}
			if table.UnAuthorizedTables.Len() == 0 {
				table.Authorized = true
			}
		}
	}
}

// MergeMaps merges the contents of src map into dst map
// If dst is nil, it will be initialized
// In case of duplicate keys, the value from src will overwrite the value in dst
func (s *AuthorizationServiceImpl) MergeMaps(dst, src *om.OrderedMap[string, *TableInfoV2]) *om.OrderedMap[string, *TableInfoV2] {
	if dst == nil {
		dst = om.NewOrderedMap[string, *TableInfoV2]()
	}
	for key, value := range src.AllFromFront() {
		dst.Set(key, value)
	}
	return dst
}

// isAuthorizedExpression checks if the given column reference and constant form an expression that authorizes the target table.
func (s *AuthorizationServiceImpl) isAuthorizedExpression(columnRef *pgquery.ColumnRef, aConst *pgquery.A_Const, targetTable *TableInfoV2, realUnAuthTable *TableInfoV2, userInfo UserContext) bool {
	// Validate: Check if the realUnAuthTable is in targetTable's unauthorized tables
	// Now, we will trust the input
	var tableAliasColumnRef string
	var columnName string
	if len(columnRef.GetFields()) == 1 {
		tableAliasColumnRef = targetTable.Alias
		columnName = s.getStringValueFromNode(columnRef.GetFields()[0])
	} else if len(columnRef.GetFields()) == 2 {
		tableAliasColumnRef = s.getStringValueFromNode(columnRef.GetFields()[0])
		columnName = s.getStringValueFromNode(columnRef.GetFields()[1])
	}

	authContext := s.getAuthorizationColumn(realUnAuthTable.Name, userInfo)
	columnInfo := targetTable.getRealColumnInfoFromAlias(columnName)
	if columnInfo == nil || columnInfo.SourceTable == nil {
		return authContext.Bypass
	}

	// If the node name is not same as target table alias
	// or the column name is not in the list of authorized columns
	if !(tableAliasColumnRef == targetTable.Alias) ||
		!ContainsString(authContext.AuthorizeColumns, columnInfo.Name) {
		return authContext.Bypass
	}

	// Check expected values
	expectedValues, _ := authContext.Conditions.Get(columnInfo.Name)
	if !ContainsInt(expectedValues, int(aConst.GetIval().Ival)) {
		return authContext.Bypass
	}
	return true
}

// getAuthorizationColumn returns the column name used for authorization filtering
// for a given table and role. Returns an empty string for public tables, admin role,
// or when no specific authorization column applies.
func (s *AuthorizationServiceImpl) getAuthorizationColumn(name string, userInfo UserContext) AuthorizationContext {
	// Admin role has unrestricted access to all tables
	if userInfo.GetRole() == "admin" {
		return AuthorizationContext{
			Bypass: true,
		}
	}

	// Public tables accessible to all roles
	switch name {
	case "program", "semester", "course", "course_program", "course_class",
		"course_class_schedule", "course_schedule_instructor", "faculty":
		return AuthorizationContext{
			Bypass: true,
		}
	}

	// Role-specific authorization columns
	switch userInfo.GetRole() {

	case "student":
		studentInfo, ok := userInfo.(*StudentInfo)
		if !ok {
			return AuthorizationContext{}
		}
		switch name {
		case "professor":
			result := AuthorizationContext{
				AuthorizeColumns: []string{"id"},
				Conditions:       om.NewOrderedMap[string, []int](),
			}
			// Create a combined slice of instructor and advisor IDs
			combinedIDs := make([]int, 0)
			combinedIDs = append(combinedIDs, studentInfo.CourseInstructorIDs...)
			combinedIDs = append(combinedIDs, studentInfo.AdvisorProfessorID)
			result.Conditions.Set("id", combinedIDs)
			return result

		case "student":
			result := AuthorizationContext{
				AuthorizeColumns: []string{"id"},
				Conditions:       om.NewOrderedMap[string, []int](),
			}
			result.Conditions.Set("id", []int{studentInfo.ID})
			return result
		case "administrative_class":
			result := AuthorizationContext{
				AuthorizeColumns: []string{"id"},
				Conditions:       om.NewOrderedMap[string, []int](),
			}
			result.Conditions.Set("id", []int{studentInfo.AdministrativeClassID})

			return result
		case "course_class_enrollment":
			result := AuthorizationContext{
				AuthorizeColumns: []string{"student_id"},
				Conditions:       om.NewOrderedMap[string, []int](),
			}
			result.Conditions.Set("student_id", []int{studentInfo.ID})

			return result
		case "student_course_class_schedule":
			result := AuthorizationContext{
				AuthorizeColumns: []string{"student_id"},
				Conditions:       om.NewOrderedMap[string, []int](),
			}
			result.Conditions.Set("student_id", []int{studentInfo.ID})
			return result
		case "student_scholarship":
			result := AuthorizationContext{
				AuthorizeColumns: []string{"student_id"},
				Conditions:       om.NewOrderedMap[string, []int](),
			}
			result.Conditions.Set("student_id", []int{studentInfo.ID})
			return result
		}
	case "professor":
		professorInfo, ok := userInfo.(*ProfessorInfo)
		if !ok {
			return AuthorizationContext{}
		}
		switch name {
		case "professor":
			result := AuthorizationContext{
				AuthorizeColumns: []string{"id"}, // Filter by professor_id
				Conditions:       om.NewOrderedMap[string, []int](),
			}
			result.Conditions.Set("id", []int{professorInfo.ID})
			return result
		case "administrative_class":
			result := AuthorizationContext{
				AuthorizeColumns: []string{"id", "advisor_id"},
				Conditions:       om.NewOrderedMap[string, []int](),
			}
			result.Conditions.Set("advisor_id", []int{professorInfo.UserInfo.ID})
			result.Conditions.Set("id", professorInfo.AdvisedClassIDs)
			return result
		case "course_class_enrollment":
			result := AuthorizationContext{
				AuthorizeColumns: []string{"course_class_id", "student_id"},
				Conditions:       om.NewOrderedMap[string, []int](),
			}

			result.Conditions.Set("course_class_id", professorInfo.TaughtCourseClassIDs)
			result.Conditions.Set("student_id", professorInfo.AdvisedStudentIDs)
			return result
		case "student_course_class_schedule":
			result := AuthorizationContext{
				AuthorizeColumns: []string{"course_class_id", "student_id"},
				Conditions:       om.NewOrderedMap[string, []int](),
			}
			result.Conditions.Set("course_class_id", professorInfo.TaughtCourseClassIDs)
			result.Conditions.Set("student_id", professorInfo.AdvisedStudentIDs)

			return result
		case "student":
			result := AuthorizationContext{
				AuthorizeColumns: []string{"id"},
				Conditions:       om.NewOrderedMap[string, []int](),
			}
			result.Conditions.Set("id", professorInfo.TaughtStudentIDs)
			return result
		}
	}

	// Default: No access for unspecified tables/roles
	return AuthorizationContext{}
}

func (s *AuthorizationServiceImpl) CheckPermission(ctx context.Context, id int, role string, table string) {
	return
}
