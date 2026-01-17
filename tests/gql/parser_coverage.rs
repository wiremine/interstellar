//! Additional parser coverage tests for gql/parser.rs
//!
//! This module covers edge cases and error paths in the GQL parser that
//! aren't covered by other test modules.

use interstellar::gql::{
    parse, parse_statement, BinaryOperator, EdgeDirection, Expression, Literal, ParseError,
    PatternElement, Statement, UnaryOperator,
};

// =============================================================================
// Parse Error Cases
// =============================================================================

mod parse_errors {
    use super::*;

    #[test]
    fn empty_query_returns_error() {
        let result = parse("");
        assert!(result.is_err());
    }

    #[test]
    fn whitespace_only_returns_error() {
        let result = parse("   \n\t  ");
        assert!(result.is_err());
    }

    #[test]
    fn unclosed_parenthesis_returns_error() {
        let result = parse("MATCH (n:Person RETURN n");
        assert!(result.is_err());
    }

    #[test]
    fn unclosed_bracket_returns_error() {
        let result = parse("MATCH (a)-[:KNOWS->(b) RETURN b");
        assert!(result.is_err());
    }

    #[test]
    fn missing_match_clause_returns_error() {
        let result = parse("RETURN n");
        assert!(result.is_err());
    }

    #[test]
    fn invalid_operator_returns_error() {
        let result = parse("MATCH (n) WHERE n.x @@ 5 RETURN n");
        assert!(result.is_err());
    }

    #[test]
    fn unterminated_string_returns_error() {
        let result = parse("MATCH (n {name: 'Alice}) RETURN n");
        assert!(result.is_err());
    }

    #[test]
    fn invalid_number_format_returns_error() {
        // Multiple dots in a number
        let result = parse("MATCH (n {x: 1.2.3}) RETURN n");
        assert!(result.is_err());
    }

    #[test]
    fn parse_union_via_parse_returns_error() {
        // parse() should return error for UNION - use parse_statement() instead
        let result = parse("MATCH (a) RETURN a UNION MATCH (b) RETURN b");
        assert!(result.is_err());
        if let Err(ParseError::Syntax(msg)) = result {
            assert!(msg.contains("parse_statement"));
        }
    }

    #[test]
    fn parse_mutation_via_parse_returns_error() {
        // parse() should return error for mutations - use parse_statement() instead
        let result = parse("CREATE (n:Person {name: 'Alice'})");
        assert!(result.is_err());
        if let Err(ParseError::Syntax(msg)) = result {
            assert!(msg.contains("parse_statement") || msg.contains("mutation"));
        }
    }

    #[test]
    fn parse_ddl_via_parse_returns_error() {
        // parse() should return error for DDL - use parse_statement() instead
        let result = parse("CREATE NODE TYPE Person ()");
        assert!(result.is_err());
        if let Err(ParseError::Syntax(msg)) = result {
            assert!(msg.contains("parse_statement") || msg.contains("DDL"));
        }
    }
}

// =============================================================================
// Statement Parsing
// =============================================================================

mod statement_parsing {
    use super::*;

    #[test]
    fn parse_statement_single_query() {
        let stmt = parse_statement("MATCH (n:Person) RETURN n").unwrap();
        assert!(matches!(stmt, Statement::Query(_)));
    }

    #[test]
    fn parse_statement_union() {
        let stmt = parse_statement("MATCH (a) RETURN a UNION MATCH (b) RETURN b").unwrap();
        if let Statement::Union { queries, all } = stmt {
            assert_eq!(queries.len(), 2);
            assert!(!all);
        } else {
            panic!("Expected Union statement");
        }
    }

    #[test]
    fn parse_statement_union_all() {
        let stmt = parse_statement("MATCH (a) RETURN a UNION ALL MATCH (b) RETURN b").unwrap();
        if let Statement::Union { queries, all } = stmt {
            assert_eq!(queries.len(), 2);
            assert!(all);
        } else {
            panic!("Expected Union statement");
        }
    }

    #[test]
    fn parse_statement_multiple_union() {
        let stmt =
            parse_statement("MATCH (a) RETURN a UNION MATCH (b) RETURN b UNION MATCH (c) RETURN c")
                .unwrap();
        if let Statement::Union { queries, .. } = stmt {
            assert_eq!(queries.len(), 3);
        } else {
            panic!("Expected Union statement with 3 queries");
        }
    }

    #[test]
    fn parse_statement_mutation() {
        let stmt = parse_statement("CREATE (n:Person {name: 'Alice'})").unwrap();
        assert!(matches!(stmt, Statement::Mutation(_)));
    }

    #[test]
    fn parse_statement_ddl() {
        let stmt = parse_statement("CREATE NODE TYPE Person ()").unwrap();
        assert!(matches!(stmt, Statement::Ddl(_)));
    }
}

// =============================================================================
// Expression Parsing
// =============================================================================

mod expression_parsing {
    use super::*;

    #[test]
    fn nested_parentheses() {
        let query = parse("MATCH (n) WHERE ((n.x > 1)) RETURN n").unwrap();
        assert!(query.where_clause.is_some());
    }

    #[test]
    fn deeply_nested_parentheses() {
        let query = parse("MATCH (n) WHERE (((n.x > 1))) RETURN n").unwrap();
        assert!(query.where_clause.is_some());
    }

    #[test]
    fn arithmetic_expression_add() {
        let query = parse("MATCH (n) RETURN n.x + n.y").unwrap();
        if let Expression::BinaryOp { op, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(op, BinaryOperator::Add));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn arithmetic_expression_subtract() {
        let query = parse("MATCH (n) RETURN n.x - n.y").unwrap();
        if let Expression::BinaryOp { op, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(op, BinaryOperator::Sub));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn arithmetic_expression_multiply() {
        let query = parse("MATCH (n) RETURN n.x * n.y").unwrap();
        if let Expression::BinaryOp { op, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(op, BinaryOperator::Mul));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn arithmetic_expression_divide() {
        let query = parse("MATCH (n) RETURN n.x / n.y").unwrap();
        if let Expression::BinaryOp { op, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(op, BinaryOperator::Div));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn arithmetic_expression_modulo() {
        let query = parse("MATCH (n) RETURN n.x % n.y").unwrap();
        if let Expression::BinaryOp { op, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(op, BinaryOperator::Mod));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn arithmetic_expression_power() {
        let query = parse("MATCH (n) RETURN n.x ^ 2").unwrap();
        if let Expression::BinaryOp { op, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(op, BinaryOperator::Pow));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn comparison_less_than() {
        let query = parse("MATCH (n) WHERE n.x < 10 RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::Lt));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn comparison_less_than_or_equal() {
        let query = parse("MATCH (n) WHERE n.x <= 10 RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::Lte));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn comparison_greater_than_or_equal() {
        let query = parse("MATCH (n) WHERE n.x >= 10 RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::Gte));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn comparison_not_equal() {
        let query = parse("MATCH (n) WHERE n.x <> 10 RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::Neq));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn comparison_not_equal_bang() {
        let query = parse("MATCH (n) WHERE n.x != 10 RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::Neq));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn unary_negation() {
        let query = parse("MATCH (n) RETURN -n.x").unwrap();
        if let Expression::UnaryOp { op, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(op, UnaryOperator::Neg));
        } else {
            panic!("Expected UnaryOp");
        }
    }

    #[test]
    fn string_contains() {
        let query = parse("MATCH (n) WHERE n.name CONTAINS 'al' RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::Contains));
        } else {
            panic!("Expected BinaryOp Contains");
        }
    }

    #[test]
    fn string_starts_with() {
        let query = parse("MATCH (n) WHERE n.name STARTS WITH 'A' RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::StartsWith));
        } else {
            panic!("Expected BinaryOp StartsWith");
        }
    }

    #[test]
    fn string_ends_with() {
        let query = parse("MATCH (n) WHERE n.name ENDS WITH 'z' RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::EndsWith));
        } else {
            panic!("Expected BinaryOp EndsWith");
        }
    }

    #[test]
    fn is_not_null() {
        let query = parse("MATCH (n) WHERE n.email IS NOT NULL RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::IsNull { negated, .. } = where_clause.expression {
            assert!(negated);
        } else {
            panic!("Expected IsNull");
        }
    }

    #[test]
    fn in_list() {
        let query = parse("MATCH (n) WHERE n.x IN [1, 2, 3] RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::InList { list, negated, .. } = where_clause.expression {
            assert!(!negated);
            assert_eq!(list.len(), 3);
        } else {
            panic!("Expected InList");
        }
    }

    #[test]
    fn not_in_list() {
        let query = parse("MATCH (n) WHERE n.x NOT IN [1, 2, 3] RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::InList { negated, .. } = where_clause.expression {
            assert!(negated);
        } else {
            panic!("Expected InList with negated=true");
        }
    }

    #[test]
    fn regex_match() {
        let query = parse("MATCH (n) WHERE n.email =~ '.*@gmail.com' RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::RegexMatch));
        } else {
            panic!("Expected RegexMatch");
        }
    }

    #[test]
    fn string_concatenation() {
        let query = parse("MATCH (n) RETURN n.first || ' ' || n.last").unwrap();
        if let Expression::BinaryOp { op, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(op, BinaryOperator::Concat));
        } else {
            panic!("Expected Concat");
        }
    }

    #[test]
    fn parameter_expression() {
        let query = parse("MATCH (n) WHERE n.id = $userId RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { right, .. } = where_clause.expression {
            if let Expression::Parameter(name) = *right {
                assert_eq!(name, "userId");
            } else {
                panic!("Expected Parameter");
            }
        } else {
            panic!("Expected BinaryOp");
        }
    }
}

// =============================================================================
// CASE Expression
// =============================================================================

mod case_expression {
    use super::*;

    #[test]
    fn case_when_then_else() {
        let query =
            parse("MATCH (n) RETURN CASE WHEN n.x > 0 THEN 'positive' ELSE 'non-positive' END")
                .unwrap();
        if let Expression::Case(case) = &query.return_clause.items[0].expression {
            assert_eq!(case.when_clauses.len(), 1);
            assert!(case.else_clause.is_some());
        } else {
            panic!("Expected Case expression");
        }
    }

    #[test]
    fn case_multiple_when_clauses() {
        let query = parse(
            "MATCH (n) RETURN CASE WHEN n.x < 0 THEN 'neg' WHEN n.x = 0 THEN 'zero' ELSE 'pos' END",
        )
        .unwrap();
        if let Expression::Case(case) = &query.return_clause.items[0].expression {
            assert_eq!(case.when_clauses.len(), 2);
            assert!(case.else_clause.is_some());
        } else {
            panic!("Expected Case expression");
        }
    }

    #[test]
    fn case_without_else() {
        let query = parse("MATCH (n) RETURN CASE WHEN n.x > 0 THEN 'positive' END").unwrap();
        if let Expression::Case(case) = &query.return_clause.items[0].expression {
            assert_eq!(case.when_clauses.len(), 1);
            assert!(case.else_clause.is_none());
        } else {
            panic!("Expected Case expression");
        }
    }
}

// =============================================================================
// EXISTS Expression
// =============================================================================

mod exists_expression {
    use super::*;

    #[test]
    fn exists_pattern() {
        let query = parse("MATCH (n) WHERE EXISTS { (n)-[:KNOWS]->() } RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::Exists { negated, pattern } = where_clause.expression {
            assert!(!negated);
            assert!(!pattern.elements.is_empty());
        } else {
            panic!("Expected Exists expression");
        }
    }

    #[test]
    fn not_exists_pattern() {
        // NOT EXISTS is parsed as UnaryOp(Not, Exists { negated: false, ... })
        let query = parse("MATCH (n) WHERE NOT EXISTS { (n)-[:KNOWS]->() } RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        // Check it's a Unary NOT operation wrapping an EXISTS
        if let Expression::UnaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, UnaryOperator::Not));
        } else {
            panic!("Expected UnaryOp Not expression");
        }
    }
}

// =============================================================================
// Function Calls
// =============================================================================

mod function_calls {
    use super::*;
    use interstellar::gql::AggregateFunc;

    #[test]
    fn count_star() {
        let query = parse("MATCH (n) RETURN COUNT(*)").unwrap();
        if let Expression::Aggregate { func, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(func, AggregateFunc::Count));
        } else {
            panic!("Expected Aggregate");
        }
    }

    #[test]
    fn count_distinct() {
        let query = parse("MATCH (n) RETURN COUNT(DISTINCT n.label)").unwrap();
        if let Expression::Aggregate { func, distinct, .. } =
            &query.return_clause.items[0].expression
        {
            assert!(matches!(func, AggregateFunc::Count));
            assert!(*distinct);
        } else {
            panic!("Expected Aggregate");
        }
    }

    #[test]
    fn sum_function() {
        let query = parse("MATCH (n) RETURN SUM(n.value)").unwrap();
        if let Expression::Aggregate { func, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(func, AggregateFunc::Sum));
        } else {
            panic!("Expected Aggregate");
        }
    }

    #[test]
    fn avg_function() {
        let query = parse("MATCH (n) RETURN AVG(n.value)").unwrap();
        if let Expression::Aggregate { func, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(func, AggregateFunc::Avg));
        } else {
            panic!("Expected Aggregate");
        }
    }

    #[test]
    fn min_function() {
        let query = parse("MATCH (n) RETURN MIN(n.value)").unwrap();
        if let Expression::Aggregate { func, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(func, AggregateFunc::Min));
        } else {
            panic!("Expected Aggregate");
        }
    }

    #[test]
    fn max_function() {
        let query = parse("MATCH (n) RETURN MAX(n.value)").unwrap();
        if let Expression::Aggregate { func, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(func, AggregateFunc::Max));
        } else {
            panic!("Expected Aggregate");
        }
    }

    #[test]
    fn collect_function() {
        let query = parse("MATCH (n) RETURN COLLECT(n.name)").unwrap();
        if let Expression::Aggregate { func, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(func, AggregateFunc::Collect));
        } else {
            panic!("Expected Aggregate");
        }
    }

    #[test]
    fn regular_function_call() {
        let query = parse("MATCH (n) RETURN toUpper(n.name)").unwrap();
        if let Expression::FunctionCall { name, args } = &query.return_clause.items[0].expression {
            assert_eq!(name, "toUpper");
            assert_eq!(args.len(), 1);
        } else {
            panic!("Expected FunctionCall");
        }
    }

    #[test]
    fn function_with_multiple_args() {
        let query = parse("MATCH (n) RETURN substring(n.name, 0, 5)").unwrap();
        if let Expression::FunctionCall { name, args } = &query.return_clause.items[0].expression {
            assert_eq!(name, "substring");
            assert_eq!(args.len(), 3);
        } else {
            panic!("Expected FunctionCall");
        }
    }
}

// =============================================================================
// List and Map Expressions
// =============================================================================

mod list_and_map_expressions {
    use super::*;

    #[test]
    fn empty_list() {
        let query = parse("MATCH (n) RETURN []").unwrap();
        if let Expression::List(items) = &query.return_clause.items[0].expression {
            assert!(items.is_empty());
        } else {
            panic!("Expected List");
        }
    }

    #[test]
    fn list_with_elements() {
        let query = parse("MATCH (n) RETURN [1, 2, 3]").unwrap();
        if let Expression::List(items) = &query.return_clause.items[0].expression {
            assert_eq!(items.len(), 3);
        } else {
            panic!("Expected List");
        }
    }

    #[test]
    fn map_literal() {
        let query = parse("MATCH (n) RETURN {name: n.name, age: n.age}").unwrap();
        if let Expression::Map(entries) = &query.return_clause.items[0].expression {
            assert_eq!(entries.len(), 2);
        } else {
            panic!("Expected Map");
        }
    }

    #[test]
    fn empty_map_literal() {
        let query = parse("MATCH (n) RETURN {}").unwrap();
        if let Expression::Map(entries) = &query.return_clause.items[0].expression {
            assert!(entries.is_empty());
        } else {
            panic!("Expected Map");
        }
    }

    #[test]
    fn index_access() {
        let query = parse("MATCH (n) RETURN n.items[0]").unwrap();
        if let Expression::Index { .. } = &query.return_clause.items[0].expression {
            // Success
        } else {
            panic!("Expected Index");
        }
    }

    #[test]
    fn slice_access() {
        let query = parse("MATCH (n) RETURN n.items[1..3]").unwrap();
        if let Expression::Slice { .. } = &query.return_clause.items[0].expression {
            // Success
        } else {
            panic!("Expected Slice");
        }
    }

    #[test]
    fn slice_with_only_start() {
        let query = parse("MATCH (n) RETURN n.items[2..]").unwrap();
        if let Expression::Slice { start, end, .. } = &query.return_clause.items[0].expression {
            assert!(start.is_some());
            assert!(end.is_none());
        } else {
            panic!("Expected Slice");
        }
    }

    #[test]
    fn slice_with_only_end() {
        let query = parse("MATCH (n) RETURN n.items[..5]").unwrap();
        if let Expression::Slice { start, end, .. } = &query.return_clause.items[0].expression {
            assert!(start.is_none());
            assert!(end.is_some());
        } else {
            panic!("Expected Slice");
        }
    }
}

// =============================================================================
// List Comprehension
// =============================================================================

mod list_comprehension {
    use super::*;

    #[test]
    fn basic_list_comprehension() {
        let query = parse("MATCH (n) RETURN [x IN n.items | x * 2]").unwrap();
        if let Expression::ListComprehension { variable, .. } =
            &query.return_clause.items[0].expression
        {
            assert_eq!(variable, "x");
        } else {
            panic!("Expected ListComprehension");
        }
    }

    #[test]
    fn list_comprehension_with_where() {
        let query = parse("MATCH (n) RETURN [x IN n.items WHERE x > 0 | x * 2]").unwrap();
        if let Expression::ListComprehension { filter, .. } =
            &query.return_clause.items[0].expression
        {
            assert!(filter.is_some());
        } else {
            panic!("Expected ListComprehension");
        }
    }
}

// =============================================================================
// Pattern Comprehension
// =============================================================================

mod pattern_comprehension {
    use super::*;

    #[test]
    fn basic_pattern_comprehension() {
        let query = parse("MATCH (n) RETURN [(n)-[:KNOWS]->(f) | f.name]").unwrap();
        if let Expression::PatternComprehension { pattern, .. } =
            &query.return_clause.items[0].expression
        {
            assert!(!pattern.elements.is_empty());
        } else {
            panic!("Expected PatternComprehension");
        }
    }

    #[test]
    fn pattern_comprehension_with_where() {
        let query =
            parse("MATCH (n) RETURN [(n)-[:KNOWS]->(f) WHERE f.age > 21 | f.name]").unwrap();
        if let Expression::PatternComprehension { filter, .. } =
            &query.return_clause.items[0].expression
        {
            assert!(filter.is_some());
        } else {
            panic!("Expected PatternComprehension");
        }
    }
}

// =============================================================================
// REDUCE Expression
// =============================================================================

mod reduce_expression {
    use super::*;

    #[test]
    fn reduce_sum() {
        let query = parse("MATCH (n) RETURN REDUCE(total = 0, x IN n.values | total + x)").unwrap();
        if let Expression::Reduce {
            accumulator,
            variable,
            ..
        } = &query.return_clause.items[0].expression
        {
            assert_eq!(accumulator, "total");
            assert_eq!(variable, "x");
        } else {
            panic!("Expected Reduce");
        }
    }
}

// =============================================================================
// List Predicates
// =============================================================================

mod list_predicates {
    use super::*;

    #[test]
    fn all_predicate() {
        let query = parse("MATCH (n) WHERE ALL(x IN n.scores WHERE x >= 60) RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::All { variable, .. } = where_clause.expression {
            assert_eq!(variable, "x");
        } else {
            panic!("Expected All");
        }
    }

    #[test]
    fn any_predicate() {
        let query = parse("MATCH (n) WHERE ANY(x IN n.tags WHERE x = 'vip') RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::Any { variable, .. } = where_clause.expression {
            assert_eq!(variable, "x");
        } else {
            panic!("Expected Any");
        }
    }

    #[test]
    fn none_predicate() {
        let query = parse("MATCH (n) WHERE NONE(x IN n.reviews WHERE x < 3) RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::None { variable, .. } = where_clause.expression {
            assert_eq!(variable, "x");
        } else {
            panic!("Expected None");
        }
    }

    #[test]
    fn single_predicate() {
        let query =
            parse("MATCH (n) WHERE SINGLE(x IN n.items WHERE x.special = true) RETURN n").unwrap();
        let where_clause = query.where_clause.unwrap();
        if let Expression::Single { variable, .. } = where_clause.expression {
            assert_eq!(variable, "x");
        } else {
            panic!("Expected Single");
        }
    }
}

// =============================================================================
// Clause Parsing
// =============================================================================

mod clause_parsing {
    use super::*;

    #[test]
    fn optional_match() {
        let query =
            parse("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(friend) RETURN n, friend")
                .unwrap();
        assert_eq!(query.optional_match_clauses.len(), 1);
    }

    #[test]
    fn multiple_optional_matches() {
        let query = parse(
            "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(f1) OPTIONAL MATCH (n)-[:LIKES]->(f2) RETURN n",
        )
        .unwrap();
        assert_eq!(query.optional_match_clauses.len(), 2);
    }

    #[test]
    fn with_path_clause() {
        let query = parse("MATCH (n) WITH PATH RETURN n").unwrap();
        assert!(query.with_path_clause.is_some());
    }

    #[test]
    fn with_path_as_alias() {
        let query = parse("MATCH (n) WITH PATH AS p RETURN p").unwrap();
        let path_clause = query.with_path_clause.unwrap();
        assert_eq!(path_clause.alias, Some("p".to_string()));
    }

    #[test]
    fn unwind_clause() {
        let query = parse("MATCH (n) UNWIND n.items AS item RETURN item").unwrap();
        assert_eq!(query.unwind_clauses.len(), 1);
        assert_eq!(query.unwind_clauses[0].alias, "item");
    }

    #[test]
    fn let_clause() {
        let query = parse("MATCH (n) LET total = n.x + n.y RETURN total").unwrap();
        assert_eq!(query.let_clauses.len(), 1);
        assert_eq!(query.let_clauses[0].variable, "total");
    }

    #[test]
    fn group_by_clause() {
        let query = parse("MATCH (n) RETURN n.label, COUNT(n) GROUP BY n.label").unwrap();
        let group_by = query.group_by_clause.unwrap();
        assert_eq!(group_by.expressions.len(), 1);
    }

    #[test]
    fn having_clause() {
        let query =
            parse("MATCH (n) RETURN n.label, COUNT(n) AS cnt GROUP BY n.label HAVING cnt > 5")
                .unwrap();
        assert!(query.having_clause.is_some());
    }

    #[test]
    fn order_by_ascending() {
        let query = parse("MATCH (n) RETURN n ORDER BY n.name ASC").unwrap();
        let order = query.order_clause.unwrap();
        assert_eq!(order.items.len(), 1);
        assert!(!order.items[0].descending);
    }

    #[test]
    fn order_by_descending() {
        let query = parse("MATCH (n) RETURN n ORDER BY n.age DESC").unwrap();
        let order = query.order_clause.unwrap();
        assert_eq!(order.items.len(), 1);
        assert!(order.items[0].descending);
    }

    #[test]
    fn order_by_multiple() {
        let query = parse("MATCH (n) RETURN n ORDER BY n.age DESC, n.name ASC").unwrap();
        let order = query.order_clause.unwrap();
        assert_eq!(order.items.len(), 2);
    }

    #[test]
    fn limit_clause() {
        let query = parse("MATCH (n) RETURN n LIMIT 10").unwrap();
        let limit = query.limit_clause.unwrap();
        assert_eq!(limit.limit, 10);
        assert!(limit.offset.is_none());
    }

    #[test]
    fn limit_with_offset() {
        let query = parse("MATCH (n) RETURN n LIMIT 10 OFFSET 5").unwrap();
        let limit = query.limit_clause.unwrap();
        assert_eq!(limit.limit, 10);
        assert_eq!(limit.offset, Some(5));
    }

    #[test]
    fn limit_with_skip() {
        let query = parse("MATCH (n) RETURN n LIMIT 10 SKIP 5").unwrap();
        let limit = query.limit_clause.unwrap();
        assert_eq!(limit.limit, 10);
        assert_eq!(limit.offset, Some(5));
    }

    #[test]
    fn return_distinct() {
        let query = parse("MATCH (n) RETURN DISTINCT n.label").unwrap();
        assert!(query.return_clause.distinct);
    }

    #[test]
    fn with_clause() {
        // WITH clause pipes results to RETURN - multiple WITH clauses are allowed
        let query = parse("MATCH (n) WITH n.x AS x RETURN x").unwrap();
        assert_eq!(query.with_clauses.len(), 1);
    }

    #[test]
    fn with_distinct() {
        let query = parse("MATCH (n) WITH DISTINCT n.label AS label RETURN label").unwrap();
        assert!(query.with_clauses[0].distinct);
    }

    #[test]
    fn with_where() {
        let query = parse("MATCH (n) WITH n WHERE n.active = true RETURN n").unwrap();
        assert!(query.with_clauses[0].where_clause.is_some());
    }

    #[test]
    fn with_order_by() {
        let query = parse("MATCH (n) WITH n ORDER BY n.age DESC RETURN n").unwrap();
        assert!(query.with_clauses[0].order_clause.is_some());
    }

    #[test]
    fn with_limit() {
        let query = parse("MATCH (n) WITH n LIMIT 10 RETURN n").unwrap();
        assert!(query.with_clauses[0].limit_clause.is_some());
    }
}

// =============================================================================
// CALL Subqueries
// =============================================================================

mod call_subqueries {
    use super::*;

    #[test]
    fn call_basic() {
        let query = parse("MATCH (n) CALL { RETURN 1 AS one } RETURN n, one").unwrap();
        assert_eq!(query.call_clauses.len(), 1);
    }

    #[test]
    fn call_with_importing() {
        let query = parse("MATCH (n) CALL { WITH n RETURN n.x AS val } RETURN n, val").unwrap();
        assert_eq!(query.call_clauses.len(), 1);
    }

    #[test]
    fn call_with_match() {
        let query = parse("MATCH (n) CALL { WITH n MATCH (n)-[:KNOWS]->(f) RETURN f } RETURN n, f")
            .unwrap();
        assert_eq!(query.call_clauses.len(), 1);
    }
}

// =============================================================================
// Edge Patterns
// =============================================================================

mod edge_patterns {
    use super::*;

    #[test]
    fn inline_where_on_node() {
        let query = parse("MATCH (n:Person WHERE n.age > 21) RETURN n").unwrap();
        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert!(node.where_clause.is_some());
        } else {
            panic!("Expected node");
        }
    }

    #[test]
    fn inline_where_on_edge() {
        let query = parse("MATCH (a)-[r:KNOWS WHERE r.since > 2020]->(b) RETURN r").unwrap();
        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            assert!(edge.where_clause.is_some());
        } else {
            panic!("Expected edge");
        }
    }

    #[test]
    fn multiple_edge_labels() {
        let query = parse("MATCH (a)-[:KNOWS:LIKES]->(b) RETURN b").unwrap();
        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            assert_eq!(edge.labels.len(), 2);
            assert!(edge.labels.contains(&"KNOWS".to_string()));
            assert!(edge.labels.contains(&"LIKES".to_string()));
        } else {
            panic!("Expected edge");
        }
    }

    #[test]
    fn edge_no_arrows_both_direction() {
        let query = parse("MATCH (a)-[r:FRIEND]-(b) RETURN r").unwrap();
        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            assert_eq!(edge.direction, EdgeDirection::Both);
        } else {
            panic!("Expected edge");
        }
    }
}

// =============================================================================
// DDL Statements
// =============================================================================

mod ddl_statements {
    use super::*;
    use interstellar::gql::DdlStatement;

    #[test]
    fn create_node_type() {
        let stmt = parse_statement("CREATE NODE TYPE Person ()").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            assert!(matches!(*ddl, DdlStatement::CreateNodeType(_)));
        } else {
            panic!("Expected DDL");
        }
    }

    #[test]
    fn create_node_type_with_properties() {
        let stmt =
            parse_statement("CREATE NODE TYPE Person (name STRING NOT NULL, age INT)").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            if let DdlStatement::CreateNodeType(node_type) = *ddl {
                assert_eq!(node_type.name, "Person");
                assert_eq!(node_type.properties.len(), 2);
                assert!(node_type.properties[0].required);
            } else {
                panic!("Expected CreateNodeType");
            }
        } else {
            panic!("Expected DDL");
        }
    }

    #[test]
    fn create_edge_type() {
        let stmt = parse_statement("CREATE EDGE TYPE KNOWS () FROM Person TO Person").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            if let DdlStatement::CreateEdgeType(edge_type) = *ddl {
                assert_eq!(edge_type.name, "KNOWS");
                assert_eq!(edge_type.from_types, vec!["Person".to_string()]);
                assert_eq!(edge_type.to_types, vec!["Person".to_string()]);
            } else {
                panic!("Expected CreateEdgeType");
            }
        } else {
            panic!("Expected DDL");
        }
    }

    #[test]
    fn alter_node_type_add_property() {
        // Correct syntax: ADD <name> <type> (no PROPERTY keyword)
        let stmt = parse_statement("ALTER NODE TYPE Person ADD email STRING").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            assert!(matches!(*ddl, DdlStatement::AlterNodeType(_)));
        } else {
            panic!("Expected DDL");
        }
    }

    #[test]
    fn alter_node_type_drop_property() {
        // Correct syntax: DROP <name> (no PROPERTY keyword)
        let stmt = parse_statement("ALTER NODE TYPE Person DROP email").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            assert!(matches!(*ddl, DdlStatement::AlterNodeType(_)));
        } else {
            panic!("Expected DDL");
        }
    }

    #[test]
    fn alter_node_type_allow_additional() {
        let stmt = parse_statement("ALTER NODE TYPE Person ALLOW ADDITIONAL PROPERTIES").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            assert!(matches!(*ddl, DdlStatement::AlterNodeType(_)));
        } else {
            panic!("Expected DDL");
        }
    }

    #[test]
    fn alter_edge_type() {
        // Correct syntax: ADD <name> <type> (no PROPERTY keyword)
        let stmt = parse_statement("ALTER EDGE TYPE KNOWS ADD since INT").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            assert!(matches!(*ddl, DdlStatement::AlterEdgeType(_)));
        } else {
            panic!("Expected DDL");
        }
    }

    #[test]
    fn drop_node_type() {
        let stmt = parse_statement("DROP NODE TYPE Person").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            assert!(matches!(*ddl, DdlStatement::DropNodeType(_)));
        } else {
            panic!("Expected DDL");
        }
    }

    #[test]
    fn drop_edge_type() {
        let stmt = parse_statement("DROP EDGE TYPE KNOWS").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            assert!(matches!(*ddl, DdlStatement::DropEdgeType(_)));
        } else {
            panic!("Expected DDL");
        }
    }

    #[test]
    fn set_validation_none() {
        let stmt = parse_statement("SET SCHEMA VALIDATION NONE").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            assert!(matches!(*ddl, DdlStatement::SetValidation(_)));
        } else {
            panic!("Expected DDL");
        }
    }

    #[test]
    fn set_validation_warn() {
        let stmt = parse_statement("SET SCHEMA VALIDATION WARN").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            assert!(matches!(*ddl, DdlStatement::SetValidation(_)));
        } else {
            panic!("Expected DDL");
        }
    }

    #[test]
    fn set_validation_strict() {
        let stmt = parse_statement("SET SCHEMA VALIDATION STRICT").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            assert!(matches!(*ddl, DdlStatement::SetValidation(_)));
        } else {
            panic!("Expected DDL");
        }
    }

    #[test]
    fn set_validation_closed() {
        let stmt = parse_statement("SET SCHEMA VALIDATION CLOSED").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            assert!(matches!(*ddl, DdlStatement::SetValidation(_)));
        } else {
            panic!("Expected DDL");
        }
    }

    #[test]
    fn property_type_list() {
        let stmt = parse_statement("CREATE NODE TYPE Tags (tags LIST)").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            if let DdlStatement::CreateNodeType(node_type) = *ddl {
                assert_eq!(node_type.properties.len(), 1);
            }
        }
    }

    #[test]
    fn property_type_map() {
        let stmt = parse_statement("CREATE NODE TYPE Metadata (data MAP)").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            if let DdlStatement::CreateNodeType(node_type) = *ddl {
                assert_eq!(node_type.properties.len(), 1);
            }
        }
    }

    #[test]
    fn property_with_default() {
        let stmt = parse_statement("CREATE NODE TYPE Person (active BOOL DEFAULT true)").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            if let DdlStatement::CreateNodeType(node_type) = *ddl {
                assert!(node_type.properties[0].default.is_some());
            }
        }
    }
}

// =============================================================================
// Mutation Statements
// =============================================================================

mod mutation_statements {
    use super::*;
    use interstellar::gql::MutationClause;

    #[test]
    fn create_vertex() {
        let stmt = parse_statement("CREATE (n:Person {name: 'Alice'})").unwrap();
        if let Statement::Mutation(m) = stmt {
            assert_eq!(m.mutations.len(), 1);
            assert!(matches!(m.mutations[0], MutationClause::Create(_)));
        } else {
            panic!("Expected Mutation");
        }
    }

    #[test]
    fn create_vertex_with_return() {
        let stmt = parse_statement("CREATE (n:Person {name: 'Alice'}) RETURN n").unwrap();
        if let Statement::Mutation(m) = stmt {
            assert!(m.return_clause.is_some());
        } else {
            panic!("Expected Mutation");
        }
    }

    #[test]
    fn match_set() {
        let stmt = parse_statement("MATCH (n:Person) SET n.age = 30").unwrap();
        if let Statement::Mutation(m) = stmt {
            assert!(m.match_clause.is_some());
            assert_eq!(m.mutations.len(), 1);
            assert!(matches!(m.mutations[0], MutationClause::Set(_)));
        } else {
            panic!("Expected Mutation");
        }
    }

    #[test]
    fn match_remove() {
        let stmt = parse_statement("MATCH (n:Person) REMOVE n.email").unwrap();
        if let Statement::Mutation(m) = stmt {
            assert_eq!(m.mutations.len(), 1);
            assert!(matches!(m.mutations[0], MutationClause::Remove(_)));
        } else {
            panic!("Expected Mutation");
        }
    }

    #[test]
    fn match_delete() {
        let stmt = parse_statement("MATCH (n:Person) DELETE n").unwrap();
        if let Statement::Mutation(m) = stmt {
            assert_eq!(m.mutations.len(), 1);
            assert!(matches!(m.mutations[0], MutationClause::Delete(_)));
        } else {
            panic!("Expected Mutation");
        }
    }

    #[test]
    fn match_detach_delete() {
        let stmt = parse_statement("MATCH (n:Person) DETACH DELETE n").unwrap();
        if let Statement::Mutation(m) = stmt {
            assert_eq!(m.mutations.len(), 1);
            assert!(matches!(m.mutations[0], MutationClause::DetachDelete(_)));
        } else {
            panic!("Expected Mutation");
        }
    }

    #[test]
    fn merge_statement() {
        let stmt = parse_statement("MERGE (n:Person {name: 'Alice'})").unwrap();
        if let Statement::Mutation(m) = stmt {
            assert_eq!(m.mutations.len(), 1);
            assert!(matches!(m.mutations[0], MutationClause::Merge(_)));
        } else {
            panic!("Expected Mutation");
        }
    }

    #[test]
    fn merge_on_create() {
        let stmt =
            parse_statement("MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = true")
                .unwrap();
        if let Statement::Mutation(m) = stmt {
            if let MutationClause::Merge(merge) = &m.mutations[0] {
                assert!(merge.on_create.is_some());
            }
        }
    }

    #[test]
    fn merge_on_match() {
        let stmt =
            parse_statement("MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.updated = true")
                .unwrap();
        if let Statement::Mutation(m) = stmt {
            if let MutationClause::Merge(merge) = &m.mutations[0] {
                assert!(merge.on_match.is_some());
            }
        }
    }

    #[test]
    fn foreach_clause() {
        // FOREACH must come after at least one mutation clause per grammar
        let stmt = parse_statement(
            "MATCH (n:Person) SET n.processed = false FOREACH (x IN n.items | SET x.processed = true)",
        )
        .unwrap();
        if let Statement::Mutation(m) = stmt {
            assert_eq!(m.foreach_clauses.len(), 1);
        } else {
            panic!("Expected Mutation");
        }
    }

    #[test]
    fn foreach_nested() {
        // FOREACH must come after at least one mutation clause per grammar
        let stmt = parse_statement(
            "MATCH (n) SET n.x = 0 FOREACH (x IN n.outer | FOREACH (y IN x.inner | SET y.visited = true))",
        )
        .unwrap();
        if let Statement::Mutation(m) = stmt {
            assert_eq!(m.foreach_clauses.len(), 1);
        } else {
            panic!("Expected Mutation");
        }
    }
}

// =============================================================================
// Literal Edge Cases
// =============================================================================

mod literal_edge_cases {
    use super::*;

    #[test]
    fn large_float() {
        // Grammar doesn't support exponent notation, but large floats work
        let query = parse("MATCH (n {x: 1500000000.5}) RETURN n").unwrap();
        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Node(node) = &pattern.elements[0] {
            if let Literal::Float(f) = &node.properties[0].1 {
                assert!((*f - 1500000000.5).abs() < 1.0);
            } else {
                panic!("Expected Float literal");
            }
        } else {
            panic!("Expected node pattern");
        }
    }

    #[test]
    fn negative_float() {
        let query = parse("MATCH (n {x: -3.14}) RETURN n").unwrap();
        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Node(node) = &pattern.elements[0] {
            if let Literal::Float(f) = &node.properties[0].1 {
                assert!((*f - (-3.14)).abs() < 0.001);
            }
        }
    }

    #[test]
    fn zero_integer() {
        let query = parse("MATCH (n {x: 0}) RETURN n").unwrap();
        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert_eq!(node.properties[0].1, Literal::Int(0));
        }
    }

    #[test]
    fn empty_string() {
        let query = parse("MATCH (n {name: ''}) RETURN n").unwrap();
        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert_eq!(node.properties[0].1, Literal::String("".to_string()));
        }
    }

    #[test]
    fn case_insensitive_boolean() {
        let query = parse("MATCH (n {active: TRUE}) RETURN n").unwrap();
        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert_eq!(node.properties[0].1, Literal::Bool(true));
        }

        let query = parse("MATCH (n {active: FALSE}) RETURN n").unwrap();
        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert_eq!(node.properties[0].1, Literal::Bool(false));
        }
    }

    #[test]
    fn case_insensitive_null() {
        let query = parse("MATCH (n {x: NULL}) RETURN n").unwrap();
        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert_eq!(node.properties[0].1, Literal::Null);
        }
    }
}
