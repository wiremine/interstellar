//! Tests for the Gremlin parser.

use super::*;

// ============================================================
// Source Step Tests
// ============================================================

#[test]
fn test_v_all() {
    let ast = parse("g.V()").unwrap();
    assert!(matches!(ast.source, SourceStep::V { ids, .. } if ids.is_empty()));
    assert!(ast.steps.is_empty());
}

#[test]
fn test_v_single_id() {
    let ast = parse("g.V(1)").unwrap();
    assert!(matches!(&ast.source, SourceStep::V { ids, .. } if ids.len() == 1));
    if let SourceStep::V { ids, .. } = &ast.source {
        assert_eq!(ids[0], Literal::Int(1));
    }
}

#[test]
fn test_v_multiple_ids() {
    let ast = parse("g.V(1, 2, 3)").unwrap();
    assert!(matches!(&ast.source, SourceStep::V { ids, .. } if ids.len() == 3));
}

#[test]
fn test_v_string_ids() {
    let ast = parse("g.V('id1', 'id2')").unwrap();
    assert!(matches!(&ast.source, SourceStep::V { ids, .. } if ids.len() == 2));
}

#[test]
fn test_e_all() {
    let ast = parse("g.E()").unwrap();
    assert!(matches!(ast.source, SourceStep::E { ids, .. } if ids.is_empty()));
}

#[test]
fn test_add_v() {
    let ast = parse("g.addV('person')").unwrap();
    assert!(matches!(&ast.source, SourceStep::AddV { label, .. } if label == "person"));
}

#[test]
fn test_add_e() {
    let ast = parse("g.addE('knows')").unwrap();
    assert!(matches!(&ast.source, SourceStep::AddE { label, .. } if label == "knows"));
}

#[test]
fn test_inject() {
    let ast = parse("g.inject(1, 2, 3)").unwrap();
    assert!(matches!(&ast.source, SourceStep::Inject { values, .. } if values.len() == 3));
}

// ============================================================
// Navigation Tests
// ============================================================

#[test]
fn test_out_no_label() {
    let ast = parse("g.V().out()").unwrap();
    assert_eq!(ast.steps.len(), 1);
    assert!(matches!(&ast.steps[0], Step::Out { labels, .. } if labels.is_empty()));
}

#[test]
fn test_out_with_label() {
    let ast = parse("g.V().out('knows')").unwrap();
    assert!(matches!(&ast.steps[0], Step::Out { labels, .. } if labels == &["knows"]));
}

#[test]
fn test_out_multiple_labels() {
    let ast = parse("g.V().out('knows', 'created')").unwrap();
    assert!(matches!(&ast.steps[0], Step::Out { labels, .. } if labels.len() == 2));
}

#[test]
fn test_in_step() {
    let ast = parse("g.V().in('knows')").unwrap();
    assert!(matches!(&ast.steps[0], Step::In { labels, .. } if labels == &["knows"]));
}

#[test]
fn test_both() {
    let ast = parse("g.V().both()").unwrap();
    assert!(matches!(&ast.steps[0], Step::Both { .. }));
}

#[test]
fn test_edge_navigation() {
    let ast = parse("g.V().outE('knows').inV()").unwrap();
    assert_eq!(ast.steps.len(), 2);
    assert!(matches!(&ast.steps[0], Step::OutE { .. }));
    assert!(matches!(&ast.steps[1], Step::InV { .. }));
}

#[test]
fn test_both_e_both_v() {
    let ast = parse("g.V().bothE().bothV()").unwrap();
    assert!(matches!(&ast.steps[0], Step::BothE { .. }));
    assert!(matches!(&ast.steps[1], Step::BothV { .. }));
}

#[test]
fn test_other_v() {
    let ast = parse("g.V().outE().otherV()").unwrap();
    assert!(matches!(&ast.steps[1], Step::OtherV { .. }));
}

// ============================================================
// Filter Tests
// ============================================================

#[test]
fn test_has_key_only() {
    let ast = parse("g.V().has('name')").unwrap();
    assert!(matches!(&ast.steps[0], Step::Has { args: HasArgs::Key(k), .. } if k == "name"));
}

#[test]
fn test_has_key_value() {
    let ast = parse("g.V().has('name', 'alice')").unwrap();
    assert!(matches!(&ast.steps[0],
        Step::Has { args: HasArgs::KeyValue { key, value: Literal::String(v) }, .. }
        if key == "name" && v == "alice"
    ));
}

#[test]
fn test_has_key_int_value() {
    let ast = parse("g.V().has('age', 30)").unwrap();
    assert!(matches!(&ast.steps[0],
        Step::Has { args: HasArgs::KeyValue { key, value: Literal::Int(30) }, .. }
        if key == "age"
    ));
}

#[test]
fn test_has_key_predicate() {
    let ast = parse("g.V().has('age', P.gt(25))").unwrap();
    assert!(matches!(&ast.steps[0],
        Step::Has { args: HasArgs::KeyPredicate { key, predicate: Predicate::Gt(_) }, .. }
        if key == "age"
    ));
}

#[test]
fn test_has_label_key_value() {
    let ast = parse("g.V().has('person', 'name', 'alice')").unwrap();
    assert!(matches!(&ast.steps[0],
        Step::Has { args: HasArgs::LabelKeyValue { label, key, value: Literal::String(_) }, .. }
        if label == "person" && key == "name"
    ));
}

#[test]
fn test_has_label() {
    let ast = parse("g.V().hasLabel('person')").unwrap();
    assert!(matches!(&ast.steps[0],
        Step::HasLabel { labels, .. } if labels == &["person"]
    ));
}

#[test]
fn test_has_label_multiple() {
    let ast = parse("g.V().hasLabel('person', 'software')").unwrap();
    assert!(matches!(&ast.steps[0],
        Step::HasLabel { labels, .. } if labels.len() == 2
    ));
}

#[test]
fn test_has_id() {
    let ast = parse("g.V().hasId(1)").unwrap();
    assert!(matches!(&ast.steps[0], Step::HasId { ids, .. } if ids.len() == 1));
}

#[test]
fn test_has_not() {
    let ast = parse("g.V().hasNot('deleted')").unwrap();
    assert!(matches!(&ast.steps[0], Step::HasNot { key, .. } if key == "deleted"));
}

#[test]
fn test_has_key() {
    let ast = parse("g.V().hasKey('name')").unwrap();
    assert!(
        matches!(&ast.steps[0], Step::HasKey { keys, .. } if keys.len() == 1 && keys[0] == "name")
    );
}

#[test]
fn test_has_key_multiple() {
    let ast = parse("g.V().hasKey('name', 'age', 'email')").unwrap();
    assert!(matches!(&ast.steps[0], Step::HasKey { keys, .. } if keys.len() == 3));
}

#[test]
fn test_has_value() {
    let ast = parse("g.V().properties().hasValue('Alice')").unwrap();
    assert!(matches!(&ast.steps[1], Step::HasValue { values, .. } if values.len() == 1));
}

#[test]
fn test_has_value_multiple() {
    let ast = parse("g.V().properties().hasValue('Alice', 'Bob', 30)").unwrap();
    assert!(matches!(&ast.steps[1], Step::HasValue { values, .. } if values.len() == 3));
}

#[test]
fn test_where_traversal() {
    let ast = parse("g.V().where(__.out('knows'))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Where {
            args: WhereArgs::Traversal(_),
            ..
        }
    ));
}

#[test]
fn test_where_predicate() {
    let ast = parse("g.V().where(P.gt(25))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Where {
            args: WhereArgs::Predicate(_),
            ..
        }
    ));
}

#[test]
fn test_is_value() {
    let ast = parse("g.V().values('age').is(30)").unwrap();
    assert!(matches!(
        &ast.steps[1],
        Step::Is {
            args: IsArgs::Value(Literal::Int(30)),
            ..
        }
    ));
}

#[test]
fn test_is_predicate() {
    let ast = parse("g.V().values('age').is(P.gt(25))").unwrap();
    assert!(matches!(
        &ast.steps[1],
        Step::Is {
            args: IsArgs::Predicate(_),
            ..
        }
    ));
}

#[test]
fn test_and_step() {
    let ast = parse("g.V().and(__.out('knows'), __.has('age', P.gt(25)))").unwrap();
    assert!(matches!(&ast.steps[0], Step::And { traversals, .. } if traversals.len() == 2));
}

#[test]
fn test_or_step() {
    let ast = parse("g.V().or(__.hasLabel('person'), __.hasLabel('software'))").unwrap();
    assert!(matches!(&ast.steps[0], Step::Or { traversals, .. } if traversals.len() == 2));
}

#[test]
fn test_not_step() {
    let ast = parse("g.V().not(__.out('knows'))").unwrap();
    assert!(matches!(&ast.steps[0], Step::Not { .. }));
}

#[test]
fn test_dedup() {
    let ast = parse("g.V().dedup()").unwrap();
    assert!(matches!(&ast.steps[0], Step::Dedup { by_label: None, .. }));
}

#[test]
fn test_limit() {
    let ast = parse("g.V().limit(10)").unwrap();
    assert!(matches!(&ast.steps[0], Step::Limit { count: 10, .. }));
}

#[test]
fn test_skip() {
    let ast = parse("g.V().skip(5)").unwrap();
    assert!(matches!(&ast.steps[0], Step::Skip { count: 5, .. }));
}

#[test]
fn test_range() {
    let ast = parse("g.V().range(5, 10)").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Range {
            start: 5,
            end: 10,
            ..
        }
    ));
}

#[test]
fn test_tail() {
    let ast = parse("g.V().tail(5)").unwrap();
    assert!(matches!(&ast.steps[0], Step::Tail { count: Some(5), .. }));
}

#[test]
fn test_coin() {
    let ast = parse("g.V().coin(0.5)").unwrap();
    assert!(
        matches!(&ast.steps[0], Step::Coin { probability, .. } if (*probability - 0.5).abs() < 0.001)
    );
}

#[test]
fn test_sample() {
    let ast = parse("g.V().sample(10)").unwrap();
    assert!(matches!(&ast.steps[0], Step::Sample { count: 10, .. }));
}

#[test]
fn test_simple_path() {
    let ast = parse("g.V().out().simplePath()").unwrap();
    assert!(matches!(&ast.steps[1], Step::SimplePath { .. }));
}

// ============================================================
// Transform Tests
// ============================================================

#[test]
fn test_values_single() {
    let ast = parse("g.V().values('name')").unwrap();
    assert!(matches!(&ast.steps[0], Step::Values { keys, .. } if keys == &["name"]));
}

#[test]
fn test_values_multiple() {
    let ast = parse("g.V().values('name', 'age')").unwrap();
    assert!(matches!(&ast.steps[0], Step::Values { keys, .. } if keys.len() == 2));
}

#[test]
fn test_value_map() {
    let ast = parse("g.V().valueMap()").unwrap();
    assert!(matches!(&ast.steps[0], Step::ValueMap { .. }));
}

#[test]
fn test_value_map_with_tokens() {
    let ast = parse("g.V().valueMap(true)").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::ValueMap {
            args: ValueMapArgs {
                include_tokens: true,
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_element_map() {
    let ast = parse("g.V().elementMap()").unwrap();
    assert!(matches!(&ast.steps[0], Step::ElementMap { .. }));
}

#[test]
fn test_id_step() {
    let ast = parse("g.V().id()").unwrap();
    assert!(matches!(&ast.steps[0], Step::Id { .. }));
}

#[test]
fn test_label_step() {
    let ast = parse("g.V().label()").unwrap();
    assert!(matches!(&ast.steps[0], Step::Label { .. }));
}

#[test]
fn test_select_single() {
    let ast = parse("g.V().as('a').out().select('a')").unwrap();
    assert!(matches!(&ast.steps[2], Step::Select { labels, .. } if labels == &["a"]));
}

#[test]
fn test_select_multiple() {
    let ast = parse("g.V().as('a').out().as('b').select('a', 'b')").unwrap();
    // Steps: as('a'), out(), as('b'), select('a', 'b') => index 3
    assert!(matches!(&ast.steps[3], Step::Select { labels, .. } if labels.len() == 2));
}

#[test]
fn test_project() {
    let ast = parse("g.V().project('name', 'age')").unwrap();
    assert!(matches!(&ast.steps[0], Step::Project { keys, .. } if keys.len() == 2));
}

#[test]
fn test_order_by() {
    let ast = parse("g.V().order().by('name')").unwrap();
    assert!(matches!(&ast.steps[0], Step::Order { .. }));
    assert!(matches!(&ast.steps[1], Step::By { args: ByArgs::Key(k), .. } if k == "name"));
}

#[test]
fn test_order_by_desc() {
    let ast = parse("g.V().order().by('age', desc)").unwrap();
    assert!(matches!(
        &ast.steps[1],
        Step::By {
            args: ByArgs::KeyOrder {
                order: OrderDirection::Desc,
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_count() {
    let ast = parse("g.V().count()").unwrap();
    assert!(matches!(&ast.steps[0], Step::Count { .. }));
}

#[test]
fn test_fold() {
    let ast = parse("g.V().fold()").unwrap();
    assert!(matches!(&ast.steps[0], Step::Fold { .. }));
}

#[test]
fn test_unfold() {
    let ast = parse("g.V().fold().unfold()").unwrap();
    assert!(matches!(&ast.steps[1], Step::Unfold { .. }));
}

#[test]
fn test_sum() {
    let ast = parse("g.V().values('age').sum()").unwrap();
    assert!(matches!(&ast.steps[1], Step::Sum { .. }));
}

#[test]
fn test_max_min_mean() {
    let ast = parse("g.V().values('age').max()").unwrap();
    assert!(matches!(&ast.steps[1], Step::Max { .. }));

    let ast = parse("g.V().values('age').min()").unwrap();
    assert!(matches!(&ast.steps[1], Step::Min { .. }));

    let ast = parse("g.V().values('age').mean()").unwrap();
    assert!(matches!(&ast.steps[1], Step::Mean { .. }));
}

#[test]
fn test_constant() {
    let ast = parse("g.V().constant('x')").unwrap();
    assert!(matches!(&ast.steps[0], Step::Constant { value: Literal::String(s), .. } if s == "x"));
}

#[test]
fn test_identity() {
    let ast = parse("g.V().identity()").unwrap();
    assert!(matches!(&ast.steps[0], Step::Identity { .. }));
}

#[test]
fn test_path() {
    let ast = parse("g.V().out().path()").unwrap();
    assert!(matches!(&ast.steps[1], Step::Path { .. }));
}

// ============================================================
// Branch Tests
// ============================================================

#[test]
fn test_union() {
    let ast = parse("g.V().union(__.out('knows'), __.out('created'))").unwrap();
    assert!(matches!(&ast.steps[0], Step::Union { traversals, .. } if traversals.len() == 2));
}

#[test]
fn test_coalesce() {
    let ast = parse("g.V().coalesce(__.values('nickname'), __.values('name'))").unwrap();
    assert!(matches!(&ast.steps[0], Step::Coalesce { traversals, .. } if traversals.len() == 2));
}

#[test]
fn test_choose_if_then_else() {
    let ast =
        parse("g.V().choose(__.hasLabel('person'), __.out('knows'), __.out('created'))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Choose {
            args: ChooseArgs::IfThenElse { .. },
            ..
        }
    ));
}

#[test]
fn test_choose_by_traversal() {
    let ast = parse("g.V().choose(__.values('type'))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Choose {
            args: ChooseArgs::ByTraversal(_),
            ..
        }
    ));
}

#[test]
fn test_optional() {
    let ast = parse("g.V().optional(__.out('knows'))").unwrap();
    assert!(matches!(&ast.steps[0], Step::Optional { .. }));
}

#[test]
fn test_local() {
    let ast = parse("g.V().local(__.out().limit(2))").unwrap();
    assert!(matches!(&ast.steps[0], Step::Local { .. }));
}

// ============================================================
// Repeat Tests
// ============================================================

#[test]
fn test_repeat_times() {
    let ast = parse("g.V().repeat(__.out()).times(3)").unwrap();
    assert_eq!(ast.steps.len(), 2);
    assert!(matches!(&ast.steps[0], Step::Repeat { .. }));
    assert!(matches!(&ast.steps[1], Step::Times { count: 3, .. }));
}

#[test]
fn test_repeat_until() {
    let ast = parse("g.V().repeat(__.out()).until(__.hasLabel('company'))").unwrap();
    assert!(matches!(&ast.steps[1], Step::Until { .. }));
}

#[test]
fn test_repeat_emit() {
    let ast = parse("g.V().repeat(__.out()).times(5).emit()").unwrap();
    assert!(matches!(
        &ast.steps[2],
        Step::Emit {
            traversal: None,
            ..
        }
    ));
}

#[test]
fn test_emit_with_condition() {
    let ast = parse("g.V().repeat(__.out()).emit(__.hasLabel('person')).times(3)").unwrap();
    assert!(matches!(
        &ast.steps[1],
        Step::Emit {
            traversal: Some(_),
            ..
        }
    ));
}

// ============================================================
// Side Effect Tests
// ============================================================

#[test]
fn test_as_step() {
    let ast = parse("g.V().as('a')").unwrap();
    assert!(matches!(&ast.steps[0], Step::As { label, .. } if label == "a"));
}

#[test]
fn test_aggregate() {
    let ast = parse("g.V().aggregate('x')").unwrap();
    assert!(matches!(&ast.steps[0], Step::Aggregate { key, .. } if key == "x"));
}

#[test]
fn test_store() {
    let ast = parse("g.V().store('x')").unwrap();
    assert!(matches!(&ast.steps[0], Step::Store { key, .. } if key == "x"));
}

#[test]
fn test_cap() {
    let ast = parse("g.V().store('x').cap('x')").unwrap();
    assert!(matches!(&ast.steps[1], Step::Cap { keys, .. } if keys == &["x"]));
}

#[test]
fn test_side_effect() {
    let ast = parse("g.V().sideEffect(__.out())").unwrap();
    assert!(matches!(&ast.steps[0], Step::SideEffect { .. }));
}

// ============================================================
// Mutation Tests
// ============================================================

#[test]
fn test_property() {
    let ast = parse("g.V().property('name', 'alice')").unwrap();
    assert!(matches!(&ast.steps[0],
        Step::Property { args: PropertyArgs { key, value: Literal::String(v), cardinality: None }, .. }
        if key == "name" && v == "alice"
    ));
}

#[test]
fn test_property_with_cardinality() {
    let ast = parse("g.V().property(single, 'name', 'alice')").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Property {
            args: PropertyArgs {
                cardinality: Some(Cardinality::Single),
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_from_label() {
    let ast = parse("g.addE('knows').from('a').to('b')").unwrap();
    assert!(matches!(&ast.steps[0], Step::From { args: FromToArgs::Label(l), .. } if l == "a"));
    assert!(matches!(&ast.steps[1], Step::To { args: FromToArgs::Label(l), .. } if l == "b"));
}

#[test]
fn test_from_traversal() {
    let ast = parse("g.V().as('a').out().addE('link').from(__.select('a'))").unwrap();
    assert!(matches!(
        &ast.steps[3],
        Step::From {
            args: FromToArgs::Traversal(_),
            ..
        }
    ));
}

#[test]
fn test_drop() {
    let ast = parse("g.V().hasLabel('temp').drop()").unwrap();
    assert!(matches!(&ast.steps[1], Step::Drop { .. }));
}

#[test]
fn test_add_v_inline() {
    let ast = parse("g.V().addV('new')").unwrap();
    assert!(matches!(&ast.steps[0], Step::AddV { label, .. } if label == "new"));
}

#[test]
fn test_add_e_inline() {
    // Note: g.V().V() is not valid in our grammar (V() is only a source step)
    // Instead test addE as an inline step in a different way
    let ast = parse("g.V().as('a').out().as('b').addE('link').from('a').to('b')").unwrap();
    // Steps: as('a'), out(), as('b'), addE('link'), from('a'), to('b')
    assert!(matches!(&ast.steps[3], Step::AddE { label, .. } if label == "link"));
}

// ============================================================
// Terminal Tests
// ============================================================

#[test]
fn test_to_list() {
    let ast = parse("g.V().toList()").unwrap();
    assert!(matches!(ast.terminal, Some(TerminalStep::ToList { .. })));
}

#[test]
fn test_to_set() {
    let ast = parse("g.V().toSet()").unwrap();
    assert!(matches!(ast.terminal, Some(TerminalStep::ToSet { .. })));
}

#[test]
fn test_next() {
    let ast = parse("g.V().next()").unwrap();
    assert!(matches!(
        ast.terminal,
        Some(TerminalStep::Next { count: None, .. })
    ));
}

#[test]
fn test_next_with_count() {
    let ast = parse("g.V().next(5)").unwrap();
    assert!(matches!(
        ast.terminal,
        Some(TerminalStep::Next { count: Some(5), .. })
    ));
}

#[test]
fn test_iterate() {
    let ast = parse("g.V().drop().iterate()").unwrap();
    assert!(matches!(ast.terminal, Some(TerminalStep::Iterate { .. })));
}

#[test]
fn test_has_next() {
    let ast = parse("g.V().hasNext()").unwrap();
    assert!(matches!(ast.terminal, Some(TerminalStep::HasNext { .. })));
}

// ============================================================
// Predicate Tests
// ============================================================

#[test]
fn test_predicate_eq() {
    let ast = parse("g.V().has('age', P.eq(30))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::Eq(_),
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_predicate_neq() {
    let ast = parse("g.V().has('status', P.neq('deleted'))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::Neq(_),
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_predicate_lt_lte() {
    let ast = parse("g.V().has('age', P.lt(30))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::Lt(_),
                ..
            },
            ..
        }
    ));

    let ast = parse("g.V().has('age', P.lte(30))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::Lte(_),
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_predicate_gt_gte() {
    let ast = parse("g.V().has('age', P.gt(30))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::Gt(_),
                ..
            },
            ..
        }
    ));

    let ast = parse("g.V().has('age', P.gte(30))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::Gte(_),
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_predicate_between() {
    let ast = parse("g.V().has('age', P.between(20, 30))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::Between { .. },
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_predicate_inside_outside() {
    let ast = parse("g.V().has('age', P.inside(20, 30))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::Inside { .. },
                ..
            },
            ..
        }
    ));

    let ast = parse("g.V().has('age', P.outside(20, 30))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::Outside { .. },
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_predicate_within() {
    let ast = parse("g.V().has('status', P.within('active', 'pending'))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::Within(_),
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_predicate_without() {
    let ast = parse("g.V().has('status', P.without('deleted', 'archived'))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::Without(_),
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_predicate_and() {
    let ast = parse("g.V().has('age', P.and(P.gte(18), P.lt(65)))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::And(_, _),
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_predicate_or() {
    let ast = parse("g.V().has('age', P.or(P.lt(18), P.gte(65)))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::Or(_, _),
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_predicate_not() {
    let ast = parse("g.V().has('status', P.not(P.eq('deleted')))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::Not(_),
                ..
            },
            ..
        }
    ));
}

// ============================================================
// TextP Tests
// ============================================================

#[test]
fn test_text_containing() {
    let ast = parse("g.V().has('name', TextP.containing('bob'))").unwrap();
    assert!(matches!(&ast.steps[0],
        Step::Has { args: HasArgs::KeyPredicate { predicate: Predicate::Containing(s), .. }, .. }
        if s == "bob"
    ));
}

#[test]
fn test_text_starting_with() {
    let ast = parse("g.V().has('name', TextP.startingWith('A'))").unwrap();
    assert!(matches!(&ast.steps[0],
        Step::Has { args: HasArgs::KeyPredicate { predicate: Predicate::StartingWith(s), .. }, .. }
        if s == "A"
    ));
}

#[test]
fn test_text_ending_with() {
    let ast = parse("g.V().has('email', TextP.endingWith('.com'))").unwrap();
    assert!(matches!(&ast.steps[0],
        Step::Has { args: HasArgs::KeyPredicate { predicate: Predicate::EndingWith(s), .. }, .. }
        if s == ".com"
    ));
}

#[test]
fn test_text_regex() {
    let ast = parse(r"g.V().has('email', TextP.regex('.*@example\.com'))").unwrap();
    assert!(matches!(
        &ast.steps[0],
        Step::Has {
            args: HasArgs::KeyPredicate {
                predicate: Predicate::Regex(_),
                ..
            },
            ..
        }
    ));
}

// ============================================================
// Complex Query Tests
// ============================================================

#[test]
fn test_complex_traversal() {
    let ast = parse(
        "g.V().hasLabel('person').has('age', P.gte(18)).out('knows').values('name').toList()",
    )
    .unwrap();
    assert_eq!(ast.steps.len(), 4);
    assert!(matches!(ast.terminal, Some(TerminalStep::ToList { .. })));
}

#[test]
fn test_chained_navigation() {
    let ast = parse("g.V().out('knows').out('created').in('created').dedup()").unwrap();
    assert_eq!(ast.steps.len(), 4);
}

#[test]
fn test_nested_anonymous_traversals() {
    let ast = parse("g.V().union(__.out('knows').out('knows'), __.out('created').in('created'))")
        .unwrap();
    if let Step::Union { traversals, .. } = &ast.steps[0] {
        assert_eq!(traversals.len(), 2);
        assert_eq!(traversals[0].steps.len(), 2);
        assert_eq!(traversals[1].steps.len(), 2);
    } else {
        panic!("Expected Union step");
    }
}

#[test]
fn test_whitespace_handling() {
    let ast = parse("g . V ( ) . hasLabel ( 'person' ) . out ( 'knows' )").unwrap();
    assert_eq!(ast.steps.len(), 2);
}

#[test]
fn test_double_quoted_strings() {
    let ast = parse(r#"g.V().has("name", "alice")"#).unwrap();
    assert!(matches!(&ast.steps[0],
        Step::Has { args: HasArgs::KeyValue { key, value: Literal::String(v) }, .. }
        if key == "name" && v == "alice"
    ));
}

// ============================================================
// Error Cases
// ============================================================

#[test]
fn test_empty_query_error() {
    let result = parse("");
    assert!(matches!(result, Err(ParseError::Empty)));
}

#[test]
fn test_whitespace_only_error() {
    let result = parse("   ");
    assert!(matches!(result, Err(ParseError::Empty)));
}

#[test]
fn test_missing_source_error() {
    let result = parse("V()");
    assert!(result.is_err());
}

#[test]
fn test_invalid_step_error() {
    let result = parse("g.V().invalidStep()");
    assert!(result.is_err());
}

// ============================================================
// Anonymous Traversal Tests
// ============================================================

#[test]
fn test_anonymous_identity() {
    let ast = parse("g.V().where(__)").unwrap();
    if let Step::Where {
        args: WhereArgs::Traversal(trav),
        ..
    } = &ast.steps[0]
    {
        assert!(trav.steps.is_empty()); // __ alone is identity
    } else {
        panic!("Expected Where step with traversal");
    }
}

#[test]
fn test_anonymous_with_steps() {
    let ast = parse("g.V().where(__.out('knows').has('age', P.gt(30)))").unwrap();
    if let Step::Where {
        args: WhereArgs::Traversal(trav),
        ..
    } = &ast.steps[0]
    {
        assert_eq!(trav.steps.len(), 2);
    } else {
        panic!("Expected Where step with traversal");
    }
}

// ============================================================
// Convenience Method Tests (Graph::query, GraphSnapshot::query)
// ============================================================

#[test]
fn test_graph_query_convenience() {
    use crate::gremlin::ExecutionResult;
    use crate::storage::Graph;
    use std::collections::HashMap;

    let graph = Graph::new();

    // Add some test data
    let alice = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert(
            "name".to_string(),
            crate::Value::String("Alice".to_string()),
        );
        props.insert("age".to_string(), crate::Value::Int(30));
        props
    });

    let bob = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), crate::Value::String("Bob".to_string()));
        props.insert("age".to_string(), crate::Value::Int(25));
        props
    });

    let _ = graph.add_edge(alice, bob, "knows", HashMap::new());

    // Test query on Graph - get all person vertices
    let result = graph.query("g.V().hasLabel('person').toList()").unwrap();
    if let ExecutionResult::List(values) = result {
        assert_eq!(values.len(), 2);
    } else {
        panic!("Expected List result");
    }

    // Test values query
    let result = graph
        .query("g.V().hasLabel('person').values('name').toList()")
        .unwrap();
    if let ExecutionResult::List(values) = result {
        assert_eq!(values.len(), 2);
        assert!(values.contains(&crate::Value::String("Alice".to_string())));
        assert!(values.contains(&crate::Value::String("Bob".to_string())));
    } else {
        panic!("Expected List result");
    }
}

#[test]
fn test_snapshot_query_convenience() {
    use crate::gremlin::ExecutionResult;
    use crate::storage::Graph;
    use std::collections::HashMap;

    let graph = Graph::new();

    // Add test data
    let alice = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert(
            "name".to_string(),
            crate::Value::String("Alice".to_string()),
        );
        props
    });

    let bob = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), crate::Value::String("Bob".to_string()));
        props
    });

    let _ = graph.add_edge(alice, bob, "knows", HashMap::new());

    // Get snapshot and query
    let snapshot = graph.snapshot();

    // Test traversal query
    let result = snapshot
        .query("g.V().hasLabel('person').out('knows').values('name').toList()")
        .unwrap();
    if let ExecutionResult::List(values) = result {
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], crate::Value::String("Bob".to_string()));
    } else {
        panic!("Expected List result");
    }

    // Test getting all vertices
    let result = snapshot.query("g.V().toList()").unwrap();
    if let ExecutionResult::List(values) = result {
        assert_eq!(values.len(), 2);
    } else {
        panic!("Expected List result");
    }
}

#[test]
fn test_query_error_handling() {
    use crate::storage::Graph;

    let graph = Graph::new();

    // Test parse error
    let result = graph.query("g.V().invalid_step()");
    assert!(result.is_err());

    // Test empty query error
    let result = graph.query("");
    assert!(result.is_err());

    // Test whitespace-only error
    let result = graph.query("   ");
    assert!(result.is_err());
}

// ============================================================
// Math Step Tests
// ============================================================

#[test]
fn test_math_simple() {
    let ast = parse("g.V().values('age').math('_ + 5')").unwrap();
    assert!(matches!(
        &ast.steps[1],
        Step::Math { expression, .. } if expression == "_ + 5"
    ));
}

#[test]
fn test_math_with_by() {
    let ast = parse("g.V().as('a').out().as('b').math('a + b').by('a').by('b')").unwrap();
    // Steps: as('a'), out(), as('b'), math('a + b'), by('a'), by('b')
    assert!(matches!(
        &ast.steps[3],
        Step::Math { expression, .. } if expression == "a + b"
    ));
    assert!(matches!(&ast.steps[4], Step::By { args: ByArgs::Key(k), .. } if k == "a"));
    assert!(matches!(&ast.steps[5], Step::By { args: ByArgs::Key(k), .. } if k == "b"));
}

#[test]
fn test_math_complex_expression() {
    let ast = parse("g.V().values('x').math('(_ * 2) + 10')").unwrap();
    assert!(matches!(
        &ast.steps[1],
        Step::Math { expression, .. } if expression == "(_ * 2) + 10"
    ));
}

// ============================================================
// Mutation Execution Tests (Graph::mutate)
// ============================================================

#[test]
fn test_mutate_add_vertex() {
    use crate::gremlin::ExecutionResult;
    use crate::storage::Graph;

    let graph = Graph::new();

    // Verify graph is empty
    assert_eq!(graph.vertex_count(), 0);

    // Add a vertex via Gremlin mutation
    let result = graph
        .mutate("g.addV('person').property('name', 'Alice')")
        .unwrap();

    // Verify mutation returned something
    if let ExecutionResult::List(values) = result {
        assert_eq!(values.len(), 1);
        // Should return a Vertex ID, not a placeholder
        assert!(
            values[0].is_vertex(),
            "Expected Vertex, got: {:?}",
            values[0]
        );
    } else {
        panic!("Expected List result");
    }

    // Verify the vertex was actually created
    assert_eq!(graph.vertex_count(), 1, "Vertex was not created");

    // Verify we can query the vertex
    let result = graph
        .query("g.V().hasLabel('person').values('name').toList()")
        .unwrap();
    if let ExecutionResult::List(names) = result {
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], crate::value::Value::String("Alice".to_string()));
    } else {
        panic!("Expected List result");
    }
}

#[test]
fn test_mutate_add_multiple_vertices() {
    use crate::gremlin::ExecutionResult;
    use crate::storage::Graph;

    let graph = Graph::new();

    // Add first vertex
    graph
        .mutate("g.addV('person').property('name', 'Alice')")
        .unwrap();
    assert_eq!(graph.vertex_count(), 1);

    // Add second vertex
    graph
        .mutate("g.addV('person').property('name', 'Bob')")
        .unwrap();
    assert_eq!(graph.vertex_count(), 2);

    // Query all names
    let result = graph
        .query("g.V().hasLabel('person').values('name').toList()")
        .unwrap();
    if let ExecutionResult::List(names) = result {
        assert_eq!(names.len(), 2);
    } else {
        panic!("Expected List result");
    }
}

#[test]
fn test_mutate_drop_vertex() {
    use crate::storage::Graph;

    let graph = Graph::new();

    // Add vertices using Rust API (since we need the IDs)
    let alice = graph.add_vertex(
        "person",
        [(
            "name".to_string(),
            crate::value::Value::String("Alice".to_string()),
        )]
        .into_iter()
        .collect(),
    );
    let _bob = graph.add_vertex(
        "person",
        [(
            "name".to_string(),
            crate::value::Value::String("Bob".to_string()),
        )]
        .into_iter()
        .collect(),
    );

    assert_eq!(graph.vertex_count(), 2);

    // Drop Alice using Gremlin
    graph.mutate(&format!("g.V({}).drop()", alice.0)).unwrap();

    // Verify only one vertex remains
    assert_eq!(graph.vertex_count(), 1);
}

#[test]
fn test_mutate_read_query_unchanged() {
    use crate::gremlin::ExecutionResult;
    use crate::storage::Graph;

    let graph = Graph::new();

    // Add vertices using Rust API
    graph.add_vertex(
        "person",
        [(
            "name".to_string(),
            crate::value::Value::String("Alice".to_string()),
        )]
        .into_iter()
        .collect(),
    );
    graph.add_vertex(
        "person",
        [(
            "name".to_string(),
            crate::value::Value::String("Bob".to_string()),
        )]
        .into_iter()
        .collect(),
    );

    // Read query via mutate() should work the same as query()
    let result = graph
        .mutate("g.V().hasLabel('person').values('name').toList()")
        .unwrap();
    if let ExecutionResult::List(values) = result {
        assert_eq!(values.len(), 2);
    } else {
        panic!("Expected List result");
    }
}

#[test]
fn test_mutate_add_edge() {
    use crate::gremlin::ExecutionResult;
    use crate::storage::Graph;

    let graph = Graph::new();

    // Create two vertices using Rust API (we need their IDs)
    let alice = graph.add_vertex(
        "person",
        [(
            "name".to_string(),
            crate::value::Value::String("Alice".to_string()),
        )]
        .into_iter()
        .collect(),
    );
    let bob = graph.add_vertex(
        "person",
        [(
            "name".to_string(),
            crate::value::Value::String("Bob".to_string()),
        )]
        .into_iter()
        .collect(),
    );

    assert_eq!(graph.vertex_count(), 2);
    assert_eq!(graph.edge_count(), 0);

    // Add edge using Gremlin mutation with vertex IDs
    let result = graph
        .mutate(&format!("g.addE('knows').from({}).to({})", alice.0, bob.0))
        .unwrap();

    // Verify mutation returned an edge
    if let ExecutionResult::List(values) = result {
        assert_eq!(values.len(), 1);
        assert!(values[0].is_edge(), "Expected Edge, got: {:?}", values[0]);
    } else {
        panic!("Expected List result");
    }

    // Verify the edge was actually created
    assert_eq!(graph.edge_count(), 1, "Edge was not created");

    // Verify we can traverse the edge
    let result = graph
        .query(&format!(
            "g.V({}).out('knows').values('name').toList()",
            alice.0
        ))
        .unwrap();
    if let ExecutionResult::List(names) = result {
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], crate::value::Value::String("Bob".to_string()));
    } else {
        panic!("Expected List result");
    }
}

#[test]
fn test_mutate_add_edge_with_labels() {
    use crate::gremlin::ExecutionResult;
    use crate::storage::Graph;

    let graph = Graph::new();

    // Add vertices and create edge using as() labels
    let alice = graph.add_vertex(
        "person",
        [(
            "name".to_string(),
            crate::value::Value::String("Alice".to_string()),
        )]
        .into_iter()
        .collect(),
    );
    let bob = graph.add_vertex(
        "person",
        [(
            "name".to_string(),
            crate::value::Value::String("Bob".to_string()),
        )]
        .into_iter()
        .collect(),
    );

    assert_eq!(graph.vertex_count(), 2);
    assert_eq!(graph.edge_count(), 0);

    // Use mid-traversal addE with as() labels (using V(id1, id2) to get both vertices)
    let result = graph
        .mutate(&format!(
            "g.V({}, {}).as('v').addE('knows').from('v').to('v')",
            alice.0, bob.0
        ))
        .unwrap();

    // The traversal iterates over two vertices, creating self-loops
    // This tests that from/to labels work, even if the result isn't ideal semantically
    if let ExecutionResult::List(values) = result {
        // Each vertex creates an edge with itself using from/to labels
        assert!(!values.is_empty(), "Expected at least one edge");
        for v in &values {
            assert!(v.is_edge(), "Expected Edge, got: {:?}", v);
        }
    } else {
        panic!("Expected List result");
    }

    // Verify edges were actually created
    assert!(
        graph.edge_count() >= 1,
        "At least one edge should be created"
    );
}

#[test]
fn test_mutate_add_edge_with_properties() {
    use crate::gremlin::ExecutionResult;
    use crate::storage::Graph;

    let graph = Graph::new();

    // Create two vertices
    let alice = graph.add_vertex(
        "person",
        [(
            "name".to_string(),
            crate::value::Value::String("Alice".to_string()),
        )]
        .into_iter()
        .collect(),
    );
    let bob = graph.add_vertex(
        "person",
        [(
            "name".to_string(),
            crate::value::Value::String("Bob".to_string()),
        )]
        .into_iter()
        .collect(),
    );

    // Add edge with properties
    let result = graph
        .mutate(&format!(
            "g.addE('knows').from({}).to({}).property('since', 2020).property('weight', 0.8)",
            alice.0, bob.0
        ))
        .unwrap();

    // Verify mutation returned an edge
    if let ExecutionResult::List(values) = result {
        assert_eq!(values.len(), 1);
        assert!(values[0].is_edge(), "Expected Edge, got: {:?}", values[0]);
    } else {
        panic!("Expected List result");
    }

    // Verify edge was created
    assert_eq!(graph.edge_count(), 1);

    // Verify edge properties via traversal
    let result = graph.query("g.E().values('since').toList()").unwrap();
    if let ExecutionResult::List(values) = result {
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], crate::value::Value::Int(2020));
    } else {
        panic!("Expected List result");
    }
}

#[test]
fn test_mutate_set_vertex_property() {
    use crate::gremlin::ExecutionResult;
    use crate::storage::Graph;

    let graph = Graph::new();

    // Create a vertex with initial properties
    let alice = graph.add_vertex(
        "person",
        [(
            "name".to_string(),
            crate::value::Value::String("Alice".to_string()),
        )]
        .into_iter()
        .collect(),
    );

    // Verify initial state
    let result = graph
        .query(&format!("g.V({}).values('age').toList()", alice.0))
        .unwrap();
    if let ExecutionResult::List(values) = result {
        assert!(values.is_empty(), "Should have no age property initially");
    }

    // Set a new property on the existing vertex
    let result = graph
        .mutate(&format!("g.V({}).property('age', 30)", alice.0))
        .unwrap();

    // Verify mutation returned the vertex
    if let ExecutionResult::List(values) = result {
        assert_eq!(values.len(), 1);
        assert!(
            values[0].is_vertex(),
            "Expected Vertex, got: {:?}",
            values[0]
        );
    } else {
        panic!("Expected List result");
    }

    // Verify the property was set
    let result = graph
        .query(&format!("g.V({}).values('age').toList()", alice.0))
        .unwrap();
    if let ExecutionResult::List(values) = result {
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], crate::value::Value::Int(30));
    } else {
        panic!("Expected List result");
    }
}

#[test]
fn test_mutate_update_vertex_property() {
    use crate::gremlin::ExecutionResult;
    use crate::storage::Graph;

    let graph = Graph::new();

    // Create a vertex with initial name
    let alice = graph.add_vertex(
        "person",
        [(
            "name".to_string(),
            crate::value::Value::String("Alice".to_string()),
        )]
        .into_iter()
        .collect(),
    );

    // Verify initial name
    let result = graph
        .query(&format!("g.V({}).values('name').toList()", alice.0))
        .unwrap();
    if let ExecutionResult::List(values) = result {
        assert_eq!(values[0], crate::value::Value::String("Alice".to_string()));
    }

    // Update the name property
    graph
        .mutate(&format!("g.V({}).property('name', 'Alicia')", alice.0))
        .unwrap();

    // Verify the property was updated
    let result = graph
        .query(&format!("g.V({}).values('name').toList()", alice.0))
        .unwrap();
    if let ExecutionResult::List(values) = result {
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], crate::value::Value::String("Alicia".to_string()));
    } else {
        panic!("Expected List result");
    }
}

#[test]
fn test_mutate_drop_edge() {
    use crate::storage::Graph;

    let graph = Graph::new();

    // Create two vertices and an edge
    let alice = graph.add_vertex(
        "person",
        [(
            "name".to_string(),
            crate::value::Value::String("Alice".to_string()),
        )]
        .into_iter()
        .collect(),
    );
    let bob = graph.add_vertex(
        "person",
        [(
            "name".to_string(),
            crate::value::Value::String("Bob".to_string()),
        )]
        .into_iter()
        .collect(),
    );

    let edge = graph
        .add_edge(
            alice,
            bob,
            "knows",
            [("since".to_string(), crate::value::Value::Int(2020))]
                .into_iter()
                .collect(),
        )
        .unwrap();

    assert_eq!(graph.vertex_count(), 2);
    assert_eq!(graph.edge_count(), 1);

    // Drop the edge using Gremlin
    graph.mutate(&format!("g.E({}).drop()", edge.0)).unwrap();

    // Verify the edge was deleted
    assert_eq!(graph.edge_count(), 0, "Edge was not dropped");
    assert_eq!(graph.vertex_count(), 2, "Vertices should still exist");
}

// ============================================================
// Script Parsing Tests
// ============================================================

#[test]
fn test_parse_script_single_statement() {
    let script = parse_script("g.V().toList()").unwrap();
    assert_eq!(script.statements.len(), 1);
    assert!(matches!(&script.statements[0], Statement::Traversal { .. }));
}

#[test]
fn test_parse_script_assignment() {
    let script = parse_script("alice = g.addV('person').next()").unwrap();
    assert_eq!(script.statements.len(), 1);
    if let Statement::Assignment { name, .. } = &script.statements[0] {
        assert_eq!(name, "alice");
    } else {
        panic!("Expected Assignment statement");
    }
}

#[test]
fn test_parse_script_multiple_statements() {
    let script = parse_script(
        r#"
        alice = g.addV('person').next()
        bob = g.addV('person').next()
        g.V().toList()
    "#,
    )
    .unwrap();
    assert_eq!(script.statements.len(), 3);
}

#[test]
fn test_parse_script_variable_in_v() {
    let script = parse_script(
        r#"
        alice = g.addV('person').next()
        g.V(alice).toList()
    "#,
    )
    .unwrap();
    assert_eq!(script.statements.len(), 2);

    // Check that the second statement has a variable reference in V()
    if let Statement::Traversal { traversal, .. } = &script.statements[1] {
        if let SourceStep::V { variable, .. } = &traversal.source {
            assert_eq!(variable.as_deref(), Some("alice"));
        } else {
            panic!("Expected V source step");
        }
    } else {
        panic!("Expected Traversal statement");
    }
}

#[test]
fn test_parse_script_variable_in_from_to() {
    let script = parse_script(
        r#"
        alice = g.addV('person').next()
        bob = g.addV('person').next()
        g.addE('knows').from(alice).to(bob).next()
    "#,
    )
    .unwrap();
    assert_eq!(script.statements.len(), 3);

    // Check the third statement has variable references
    if let Statement::Traversal { traversal, .. } = &script.statements[2] {
        // Find the from and to steps
        let mut found_from = false;
        let mut found_to = false;
        for step in &traversal.steps {
            match step {
                Step::From { args, .. } => {
                    assert!(matches!(args, FromToArgs::Variable(v) if v == "alice"));
                    found_from = true;
                }
                Step::To { args, .. } => {
                    assert!(matches!(args, FromToArgs::Variable(v) if v == "bob"));
                    found_to = true;
                }
                _ => {}
            }
        }
        assert!(found_from, "Expected from(alice)");
        assert!(found_to, "Expected to(bob)");
    } else {
        panic!("Expected Traversal statement");
    }
}

// ============================================================
// Script Execution Tests
// ============================================================

#[test]
fn test_execute_script_basic_workflow() {
    let graph = crate::storage::Graph::new();

    let script_result = graph
        .execute_script(
            r#"
        alice = g.addV('person').property('name', 'Alice').next()
        bob = g.addV('person').property('name', 'Bob').next()
        g.addE('knows').from(alice).to(bob).next()
        g.V(alice).out('knows').values('name').toList()
    "#,
        )
        .unwrap();

    // The result should be Bob's name
    if let ExecutionResult::List(names) = script_result.result {
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], crate::value::Value::String("Bob".to_string()));
    } else {
        panic!("Expected List result, got {:?}", script_result.result);
    }

    // Verify variables are returned
    assert!(script_result.variables.contains("alice"));
    assert!(script_result.variables.contains("bob"));

    // Verify graph state
    assert_eq!(graph.vertex_count(), 2);
    assert_eq!(graph.edge_count(), 1);
}

#[test]
fn test_execute_script_variable_reference_in_v() {
    let graph = crate::storage::Graph::new();

    let script_result = graph
        .execute_script(
            r#"
        v1 = g.addV('person').property('name', 'Test').next()
        g.V(v1).values('name').toList()
    "#,
        )
        .unwrap();

    if let ExecutionResult::List(names) = script_result.result {
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], crate::value::Value::String("Test".to_string()));
    } else {
        panic!("Expected List result");
    }
}

#[test]
fn test_execute_script_multiple_edges() {
    let graph = crate::storage::Graph::new();

    let script_result = graph
        .execute_script(
            r#"
        alice = g.addV('person').property('name', 'Alice').next()
        bob = g.addV('person').property('name', 'Bob').next()
        charlie = g.addV('person').property('name', 'Charlie').next()
        g.addE('knows').from(alice).to(bob).next()
        g.addE('knows').from(alice).to(charlie).next()
        g.V(alice).out('knows').values('name').toList()
    "#,
        )
        .unwrap();

    if let ExecutionResult::List(names) = script_result.result {
        assert_eq!(names.len(), 2);
        let name_strings: Vec<String> = names
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert!(name_strings.contains(&"Bob".to_string()));
        assert!(name_strings.contains(&"Charlie".to_string()));
    } else {
        panic!("Expected List result");
    }
}

#[test]
fn test_execute_script_empty_result() {
    let graph = crate::storage::Graph::new();

    // Create vertices but query for non-existent edges
    let script_result = graph
        .execute_script(
            r#"
        alice = g.addV('person').property('name', 'Alice').next()
        g.V(alice).out('knows').values('name').toList()
    "#,
        )
        .unwrap();

    if let ExecutionResult::List(names) = script_result.result {
        assert!(names.is_empty());
    } else {
        panic!("Expected empty List result");
    }
}

#[test]
fn test_execute_script_repl_style_workflow() {
    use crate::gremlin::VariableContext;

    let graph = crate::storage::Graph::new();

    // First REPL command
    let result1 = graph
        .execute_script_with_context(
            "alice = g.addV('person').property('name', 'Alice').next()",
            VariableContext::new(),
        )
        .unwrap();

    assert!(result1.variables.contains("alice"));
    let ctx = result1.variables;

    // Second REPL command (uses alice from previous)
    let result2 = graph
        .execute_script_with_context("bob = g.addV('person').property('name', 'Bob').next()", ctx)
        .unwrap();

    assert!(result2.variables.contains("alice")); // alice persists
    assert!(result2.variables.contains("bob")); // bob added
    let ctx = result2.variables;

    // Third REPL command (uses both alice and bob)
    let result3 = graph
        .execute_script_with_context("g.addE('knows').from(alice).to(bob).next()", ctx)
        .unwrap();

    let ctx = result3.variables;

    // Fourth REPL command (query using alice)
    let result4 = graph
        .execute_script_with_context("g.V(alice).out('knows').values('name').toList()", ctx)
        .unwrap();

    if let ExecutionResult::List(names) = result4.result {
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], crate::value::Value::String("Bob".to_string()));
    } else {
        panic!("Expected List result");
    }
}

#[test]
fn test_script_result_variables_are_accessible() {
    let graph = crate::storage::Graph::new();

    let script_result = graph
        .execute_script(
            r#"
        a = g.addV('test').next()
        b = g.addV('test').next()
        c = g.addV('test').next()
    "#,
        )
        .unwrap();

    // Check all variables are present
    assert_eq!(script_result.variables.len(), 3);
    assert!(script_result.variables.contains("a"));
    assert!(script_result.variables.contains("b"));
    assert!(script_result.variables.contains("c"));

    // Check values are vertex IDs
    assert!(script_result.variables.get_vertex_id("a").is_some());
    assert!(script_result.variables.get_vertex_id("b").is_some());
    assert!(script_result.variables.get_vertex_id("c").is_some());
}

#[test]
fn test_variable_context_basic() {
    use crate::gremlin::VariableContext;
    use crate::value::Value;

    let mut ctx = VariableContext::new();

    // Test bind and get
    ctx.bind("test".to_string(), Value::Int(42));
    assert!(ctx.contains("test"));
    assert_eq!(ctx.get("test"), Some(&Value::Int(42)));

    // Test get_vertex_id with Int
    ctx.bind("vid".to_string(), Value::Int(123));
    assert_eq!(ctx.get_vertex_id("vid"), Some(crate::value::VertexId(123)));

    // Test variables iterator
    let vars: Vec<&str> = ctx.variables().collect();
    assert!(vars.contains(&"test"));
    assert!(vars.contains(&"vid"));
}
