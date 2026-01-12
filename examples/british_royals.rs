//! British Royal Family Graph Example
//!
//! This example demonstrates graph traversal queries on a real-world family tree dataset
//! containing 70+ members of the British Royal Family from Queen Victoria to the present day.
//!
//! The dataset includes:
//! - Person vertices with properties: name, birth_date, death_date, house, is_monarch, etc.
//! - Relationship edges: parent_of, child_of, married_to
//!
//! This example showcases:
//! - Data loading from JSON fixtures
//! - Basic traversal with filtering (Phase 3)
//! - Navigation steps: out(), in_(), both(), other_v() (Phase 3 & 7)
//! - Predicate system: p::eq, p::gt, p::within, p::containing (Phase 4)
//! - Filter steps: has_not(), is_(), is_eq() (Phase 7)
//! - Anonymous traversals: __::out(), __::has_label() (Phase 4)
//! - Branch steps: union(), coalesce(), choose(), optional() (Phase 5)
//! - Repeat steps for ancestry/descendant queries (Phase 5)
//! - Path tracking with as_() and select() (Phase 3)
//! - Transform steps: value_map(), element_map(), unfold(), order() (Phase 7)
//! - Aggregation steps: group(), group_count() (Phase 7)
//!
//! Run with: `cargo run --example british_royals`

use intersteller::graph::Graph;
use intersteller::storage::{GraphStorage, InMemoryGraph};
use intersteller::traversal::{p, __};
use intersteller::value::{Value, VertexId};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

// =============================================================================
// Data Loading
// =============================================================================

/// Load the British Royals JSON fixture and build the graph.
fn load_royals_graph() -> (Graph, Arc<InMemoryGraph>, HashMap<String, VertexId>) {
    let json_str = fs::read_to_string("examples/fixtures/british_royals.json")
        .expect("Failed to read british_royals.json");
    let data: JsonValue = serde_json::from_str(&json_str).expect("Failed to parse JSON");

    let mut storage = InMemoryGraph::new();
    let mut person_ids: HashMap<String, VertexId> = HashMap::new();

    // Load all persons as vertices
    if let Some(persons) = data["persons"].as_array() {
        for person in persons {
            let json_id = person["id"].as_str().unwrap_or("unknown");

            let mut props = HashMap::new();

            // Required string properties
            if let Some(v) = person["name"].as_str() {
                props.insert("name".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = person["full_name"].as_str() {
                props.insert("full_name".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = person["house"].as_str() {
                props.insert("house".to_string(), Value::String(v.to_string()));
            }

            // Date properties (stored as strings for simplicity)
            if let Some(v) = person["birth_date"].as_str() {
                props.insert("birth_date".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = person["birth_country"].as_str() {
                props.insert("birth_country".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = person["birth_city"].as_str() {
                props.insert("birth_city".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = person["death_date"].as_str() {
                props.insert("death_date".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = person["death_country"].as_str() {
                props.insert("death_country".to_string(), Value::String(v.to_string()));
            }

            // Boolean properties
            if let Some(v) = person["is_monarch"].as_bool() {
                props.insert("is_monarch".to_string(), Value::Bool(v));
            }
            if let Some(v) = person["abdicated"].as_bool() {
                props.insert("abdicated".to_string(), Value::Bool(v));
            }

            // Reign dates for monarchs
            if let Some(v) = person["reign_start"].as_str() {
                props.insert("reign_start".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = person["reign_end"].as_str() {
                props.insert("reign_end".to_string(), Value::String(v.to_string()));
            }

            // Titles as a list
            if let Some(titles) = person["titles"].as_array() {
                let title_values: Vec<Value> = titles
                    .iter()
                    .filter_map(|t| t.as_str().map(|s| Value::String(s.to_string())))
                    .collect();
                if !title_values.is_empty() {
                    props.insert("titles".to_string(), Value::List(title_values));
                }
            }

            // Store original JSON ID for lookups
            props.insert("json_id".to_string(), Value::String(json_id.to_string()));

            let vid = storage.add_vertex("person", props);
            person_ids.insert(json_id.to_string(), vid);
        }
    }

    // Load parent-child relationships
    if let Some(relations) = data["parent_child"].as_array() {
        for rel in relations {
            let parent_json_id = rel["parent_id"].as_str().unwrap_or("");
            let child_json_id = rel["child_id"].as_str().unwrap_or("");
            let relationship = rel["relationship"].as_str().unwrap_or("");

            if let (Some(&parent_vid), Some(&child_vid)) = (
                person_ids.get(parent_json_id),
                person_ids.get(child_json_id),
            ) {
                // parent_of edge from parent to child
                let mut props = HashMap::new();
                props.insert(
                    "relationship".to_string(),
                    Value::String(relationship.to_string()),
                );
                let _ = storage.add_edge(parent_vid, child_vid, "parent_of", props.clone());

                // child_of edge from child to parent (for reverse traversal)
                let _ = storage.add_edge(child_vid, parent_vid, "child_of", props);
            }
        }
    }

    // Load marriages
    if let Some(marriages) = data["marriages"].as_array() {
        for marriage in marriages {
            let p1_json_id = marriage["person1_id"].as_str().unwrap_or("");
            let p2_json_id = marriage["person2_id"].as_str().unwrap_or("");

            if let (Some(&p1_vid), Some(&p2_vid)) =
                (person_ids.get(p1_json_id), person_ids.get(p2_json_id))
            {
                let mut props = HashMap::new();
                if let Some(date) = marriage["marriage_date"].as_str() {
                    props.insert("marriage_date".to_string(), Value::String(date.to_string()));
                }
                if let Some(city) = marriage["marriage_city"].as_str() {
                    props.insert("marriage_city".to_string(), Value::String(city.to_string()));
                }
                if let Some(reason) = marriage["end_reason"].as_str() {
                    props.insert("end_reason".to_string(), Value::String(reason.to_string()));
                }
                if let Some(divorce) = marriage["divorce_date"].as_str() {
                    props.insert(
                        "divorce_date".to_string(),
                        Value::String(divorce.to_string()),
                    );
                }

                // Bidirectional marriage edges
                let _ = storage.add_edge(p1_vid, p2_vid, "married_to", props.clone());
                let _ = storage.add_edge(p2_vid, p1_vid, "married_to", props);
            }
        }
    }

    let storage = Arc::new(storage);
    let graph = Graph::new(storage.clone());

    (graph, storage, person_ids)
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Get name from a vertex value
fn get_name(storage: &Arc<InMemoryGraph>, value: &Value) -> String {
    if let Some(vid) = value.as_vertex_id() {
        if let Some(vertex) = storage.get_vertex(vid) {
            if let Some(Value::String(name)) = vertex.properties.get("name") {
                return name.clone();
            }
        }
    }
    format!("{:?}", value)
}

/// Display a list of vertex results as names
fn display_names(storage: &Arc<InMemoryGraph>, results: &[Value]) -> String {
    if results.is_empty() {
        return "(none)".to_string();
    }
    results
        .iter()
        .map(|v| get_name(storage, v))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Display a vertex with additional property
fn display_with_prop(storage: &Arc<InMemoryGraph>, results: &[Value], prop: &str) -> String {
    if results.is_empty() {
        return "(none)".to_string();
    }
    results
        .iter()
        .map(|v| {
            if let Some(vid) = v.as_vertex_id() {
                if let Some(vertex) = storage.get_vertex(vid) {
                    let name = vertex
                        .properties
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("?");
                    let prop_val = vertex
                        .properties
                        .get(prop)
                        .map(|v| format!("{:?}", v))
                        .unwrap_or_else(|| "N/A".to_string());
                    return format!("{} ({}={})", name, prop, prop_val);
                }
            }
            format!("{:?}", v)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Format a Value for display
fn format_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => format!("{:.1}", f),
        Value::Bool(b) => b.to_string(),
        Value::List(items) => {
            let formatted: Vec<String> = items.iter().map(format_value).collect();
            format!("[{}]", formatted.join(", "))
        }
        Value::Map(map) => {
            let pairs: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, format_value(v)))
                .collect();
            format!("{{{}}}", pairs.join(", "))
        }
        Value::Null => "null".to_string(),
        Value::Vertex(vid) => format!("v[{:?}]", vid),
        Value::Edge(eid) => format!("e[{:?}]", eid),
    }
}

fn print_section(title: &str) {
    println!("\n{}", "=".repeat(70));
    println!("{}", title);
    println!("{}", "=".repeat(70));
}

fn print_query(description: &str) {
    println!("\n--- {} ---", description);
}

// =============================================================================
// Query Demonstrations
// =============================================================================

fn main() {
    println!("=== British Royal Family Graph Database Example ===");
    println!("Loading data from examples/fixtures/british_royals.json...\n");

    let (graph, storage, _person_ids) = load_royals_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // Report graph statistics
    let vertex_count = g.v().count();
    let edge_count = g.e().count();
    println!("Graph loaded successfully!");
    println!("  Vertices: {}", vertex_count);
    println!("  Edges: {}", edge_count);

    // =========================================================================
    // SECTION 1: Basic Queries
    // =========================================================================
    print_section("1. BASIC QUERIES");

    // Query 1: Find all monarchs
    print_query("Find all British monarchs");
    let monarchs = g
        .v()
        .has_label("person")
        .has_value("is_monarch", true)
        .to_list();
    println!(
        "Monarchs ({}): {}",
        monarchs.len(),
        display_names(&storage, &monarchs)
    );

    // Query 2: Find living royals using has_not() (Phase 7)
    print_query("Find living royals using has_not()");
    let living = g
        .v()
        .has_label("person")
        .has_not("death_date") // Phase 7: cleaner than not(__::has("death_date"))
        .to_list();
    println!(
        "Living royals ({}): {}",
        living.len(),
        display_names(&storage, &living)
    );

    // Query 3: Find members of House Windsor
    print_query("Find members of House Windsor");
    let windsor = g
        .v()
        .has_label("person")
        .has_value("house", "Windsor")
        .to_list();
    println!(
        "House Windsor ({}): {}",
        windsor.len(),
        display_names(&storage, &windsor)
    );

    // =========================================================================
    // SECTION 2: Navigation Queries
    // =========================================================================
    print_section("2. NAVIGATION QUERIES");

    // Query 4: Find Elizabeth II's children
    print_query("Find Elizabeth II's children");
    let elizabeth_children = g
        .v()
        .has_value("name", "Elizabeth II")
        .out_labels(&["parent_of"])
        .to_list();
    println!(
        "Elizabeth II's children: {}",
        display_names(&storage, &elizabeth_children)
    );

    // Query 5: Find Prince William's parents
    print_query("Find Prince William's parents");
    let william_parents = g
        .v()
        .has_value("name", "Prince William")
        .out_labels(&["child_of"])
        .to_list();
    println!(
        "Prince William's parents: {}",
        display_names(&storage, &william_parents)
    );

    // Query 6: Find Charles III's spouses (past and present)
    print_query("Find Charles III's spouses");
    let charles_spouses = g
        .v()
        .has_value("name", "Charles III")
        .out_labels(&["married_to"])
        .to_list();
    println!(
        "Charles III's spouses: {}",
        display_names(&storage, &charles_spouses)
    );

    // Query 7: Find all grandchildren of Elizabeth II
    print_query("Find all grandchildren of Elizabeth II");
    let grandchildren = g
        .v()
        .has_value("name", "Elizabeth II")
        .out_labels(&["parent_of"])
        .out_labels(&["parent_of"])
        .to_list();
    println!(
        "Elizabeth II's grandchildren ({}): {}",
        grandchildren.len(),
        display_names(&storage, &grandchildren)
    );

    // Query 7b: Navigate using other_v() from marriage edges (Phase 7)
    print_query("Find Charles III's spouses via edge navigation using other_v()");
    let charles_spouses_via_edge = g
        .v()
        .has_value("name", "Charles III")
        .out_e_labels(&["married_to"]) // Get marriage edges
        .other_v() // Navigate to the other vertex (spouse)
        .to_list();
    println!(
        "Charles III's spouses (via other_v): {}",
        display_names(&storage, &charles_spouses_via_edge)
    );

    // =========================================================================
    // SECTION 3: Predicate Queries
    // =========================================================================
    print_section("3. PREDICATE QUERIES (p:: module)");

    // Query 8: Find royals born before 1900
    print_query("Find royals born before 1900");
    let born_before_1900 = g
        .v()
        .has_label("person")
        .has_where("birth_date", p::lt("1900-01-01"))
        .to_list();
    println!(
        "Born before 1900 ({}): {}",
        born_before_1900.len(),
        display_names(&storage, &born_before_1900)
    );

    // Query 9: Find royals from specific houses
    print_query("Find royals from House Hanover or Saxe-Coburg and Gotha");
    let old_houses = g
        .v()
        .has_label("person")
        .has_where("house", p::within(["Hanover", "Saxe-Coburg and Gotha"]))
        .to_list();
    println!(
        "Old royal houses ({}): {}",
        old_houses.len(),
        display_with_prop(&storage, &old_houses, "house")
    );

    // Query 10: Find royals with "Elizabeth" in their name
    print_query("Find royals with 'Elizabeth' in their name");
    let elizabeths = g
        .v()
        .has_label("person")
        .has_where("name", p::containing("Elizabeth"))
        .to_list();
    println!(
        "Names containing 'Elizabeth': {}",
        display_names(&storage, &elizabeths)
    );

    // Query 11: Find royals born in specific countries
    print_query("Find royals NOT born in England");
    let not_english_born = g
        .v()
        .has_label("person")
        .has_where("birth_country", p::neq("England"))
        .to_list();
    println!(
        "Not born in England ({}): {}",
        not_english_born.len(),
        display_with_prop(&storage, &not_english_born, "birth_country")
    );

    // Query 11b: Using is_() to filter extracted values (Phase 7)
    print_query("Filter house values using is_eq()");
    let windsor_count = g
        .v()
        .has_label("person")
        .values("house")
        .is_eq("Windsor") // Filter to exact value
        .count();
    println!(
        "Count of Windsor house members (via is_eq): {}",
        windsor_count
    );

    // Query 11c: Using is_() with predicate on extracted dates (Phase 7)
    print_query("Filter birth dates using is_() with predicate");
    let early_births = g
        .v()
        .has_label("person")
        .values("birth_date")
        .is_(p::lt("1900-01-01")) // Filter dates before 1900
        .count();
    println!("Royals born before 1900 (via is_): {}", early_births);

    // =========================================================================
    // SECTION 4: Anonymous Traversal Queries
    // =========================================================================
    print_section("4. ANONYMOUS TRAVERSAL QUERIES (__:: module)");

    // Query 12: Find people who have children
    print_query("Find royals who are parents (have outgoing parent_of edges)");
    let parents = g
        .v()
        .has_label("person")
        .where_(__::out_labels(&["parent_of"]))
        .to_list();
    println!(
        "Royals with children ({}): {}",
        parents.len(),
        display_names(&storage, &parents)
    );

    // Query 13: Find people who have NO children (leaf nodes in family tree)
    print_query("Find royals without children");
    let childless = g
        .v()
        .has_label("person")
        .not(__::out_labels(&["parent_of"]))
        .to_list();
    println!(
        "Royals without children ({}): {}",
        childless.len(),
        display_names(&storage, &childless)
    );

    // Query 14: Find monarchs who were married
    print_query("Find monarchs who were married");
    let married_monarchs = g
        .v()
        .has_label("person")
        .has_value("is_monarch", true)
        .where_(__::out_labels(&["married_to"]))
        .to_list();
    println!(
        "Married monarchs: {}",
        display_names(&storage, &married_monarchs)
    );

    // =========================================================================
    // SECTION 5: Branch Step Queries
    // =========================================================================
    print_section("5. BRANCH STEP QUERIES (union, coalesce, choose)");

    // Query 15: Union - Get both parents AND children of Charles III
    print_query("Get Charles III's parents AND children (using union)");
    let charles_family = g
        .v()
        .has_value("name", "Charles III")
        .union(vec![
            __::out_labels(&["child_of"]),  // parents
            __::out_labels(&["parent_of"]), // children
        ])
        .dedup()
        .to_list();
    println!(
        "Charles III's parents and children: {}",
        display_names(&storage, &charles_family)
    );

    // Query 16: Coalesce - Get full_name if available, otherwise name
    print_query("Get reign_end for Elizabeth II (coalesce with default)");
    let _reign_info = g
        .v()
        .has_value("name", "Elizabeth II")
        .coalesce(vec![
            __::values("reign_end"),
            __::constant("Still reigning"),
        ])
        .to_list();
    // Note: Elizabeth II does have reign_end, so this will return that date

    // Query 17: Choose - Different traversal based on monarch status
    print_query("Conditional: If monarch, get reign_start; else get birth_date");
    let conditional = g
        .v()
        .has_value("name", "Elizabeth II")
        .choose(
            __::has_value("is_monarch", true),
            __::values("reign_start"),
            __::values("birth_date"),
        )
        .to_list();
    println!("Elizabeth II (monarch path): {:?}", conditional);

    let conditional2 = g
        .v()
        .has_value("name", "Prince Philip")
        .choose(
            __::has_value("is_monarch", true),
            __::values("reign_start"),
            __::values("birth_date"),
        )
        .to_list();
    println!("Prince Philip (non-monarch path): {:?}", conditional2);

    // Query 18: Optional - Try to get spouse, keep person if unmarried
    print_query("Get spouse if married, otherwise keep the person (using optional)");
    let with_optional = g
        .v()
        .has_value("name", "Princess Victoria") // Never married
        .optional(__::out_labels(&["married_to"]))
        .to_list();
    println!(
        "Princess Victoria with optional spouse: {}",
        display_names(&storage, &with_optional)
    );

    // =========================================================================
    // SECTION 6: Repeat Step Queries (Ancestry/Lineage)
    // =========================================================================
    print_section("6. REPEAT STEP QUERIES (Ancestry & Lineage)");

    // Query 19: Find all ancestors of Prince George (up to 4 generations)
    print_query("Find Prince George's ancestors (up to 4 generations)");
    let george_ancestors = g
        .v()
        .has_value("name", "Prince George")
        .repeat(__::out_labels(&["child_of"]))
        .times(4)
        .emit()
        .dedup()
        .to_list();
    println!(
        "Prince George's ancestors ({}): {}",
        george_ancestors.len(),
        display_names(&storage, &george_ancestors)
    );

    // Query 20: Find the line of succession from Victoria
    print_query("Trace lineage: Victoria -> Edward VII -> ... -> Charles III");
    // This finds the direct line through monarchs
    let victoria_line = g
        .v()
        .has_value("name", "Victoria")
        .repeat(__::out_labels(&["parent_of"]).has_value("is_monarch", true))
        .times(6)
        .emit()
        .to_list();
    println!(
        "Victoria's monarch descendants: {}",
        display_names(&storage, &victoria_line)
    );

    // Query 21: Find all descendants of Queen Victoria (3 generations)
    print_query("Find Queen Victoria's descendants (3 generations)");
    let victoria_descendants = g
        .v()
        .has_value("name", "Victoria")
        .repeat(__::out_labels(&["parent_of"]))
        .times(3)
        .emit()
        .dedup()
        .to_list();
    println!(
        "Victoria's descendants - 3 gen ({}): {}",
        victoria_descendants.len(),
        display_names(&storage, &victoria_descendants)
    );

    // Query 22: Find common ancestor path - who are the ancestors shared by William and Harry?
    print_query("Find Prince William's ancestors (showing path)");
    let william_ancestor_path = g
        .v()
        .has_value("name", "Prince William")
        .repeat(__::out_labels(&["child_of"]))
        .times(3)
        .emit()
        .emit_first()
        .to_list();
    println!(
        "William's lineage (with self): {}",
        display_names(&storage, &william_ancestor_path)
    );

    // =========================================================================
    // SECTION 7: Path Tracking Queries
    // =========================================================================
    print_section("7. PATH TRACKING QUERIES (as_, select, path)");

    // Query 23: Track parent-child relationship with labels
    print_query("Track parent-child with labeled positions");
    let labeled_path = g
        .v()
        .has_value("name", "Prince William")
        .as_("child")
        .out_labels(&["child_of"])
        .as_("parent")
        .select(&["child", "parent"])
        .to_list();
    println!("William -> Parent mappings:");
    for result in &labeled_path {
        if let Value::Map(map) = result {
            let child = map
                .get("child")
                .map(|v| get_name(&storage, v))
                .unwrap_or_default();
            let parent = map
                .get("parent")
                .map(|v| get_name(&storage, v))
                .unwrap_or_default();
            println!("  {} -> {}", child, parent);
        }
    }

    // Query 24: Get full traversal path from Charles to his grandchildren
    print_query("Full path: Charles III -> child -> grandchild");
    let path_results = g
        .v()
        .has_value("name", "Charles III")
        .with_path() // Enable path tracking
        .out_labels(&["parent_of"])
        .out_labels(&["parent_of"])
        .path()
        .to_list();
    println!("Paths from Charles III to grandchildren:");
    for (i, path_value) in path_results.iter().enumerate() {
        if let Value::List(path) = path_value {
            let names: Vec<String> = path.iter().map(|v| get_name(&storage, v)).collect();
            println!("  Path {}: {}", i + 1, names.join(" -> "));
        }
    }

    // =========================================================================
    // SECTION 8: Complex Combined Queries
    // =========================================================================
    print_section("8. COMPLEX COMBINED QUERIES");

    // Query 25: Find all living descendants of Elizabeth II who are NOT monarchs
    print_query("Living descendants of Elizabeth II who are not monarchs");
    let living_non_monarch_descendants = g
        .v()
        .has_value("name", "Elizabeth II")
        .repeat(__::out_labels(&["parent_of"]))
        .times(3)
        .emit()
        .not(__::has("death_date"))
        .not(__::has_value("is_monarch", true))
        .dedup()
        .to_list();
    println!(
        "Living non-monarch descendants ({}): {}",
        living_non_monarch_descendants.len(),
        display_names(&storage, &living_non_monarch_descendants)
    );

    // Query 26: Find the monarch who abdicated
    print_query("Find the monarch who abdicated");
    let abdicated = g
        .v()
        .has_label("person")
        .has_value("is_monarch", true)
        .has_value("abdicated", true)
        .to_list();
    println!(
        "Abdicated monarch: {}",
        display_with_prop(&storage, &abdicated, "reign_end")
    );

    // Query 27: Count children per monarch
    print_query("Which monarchs had children?");
    let monarchs_with_kids = g
        .v()
        .has_label("person")
        .has_value("is_monarch", true)
        .where_(__::out_labels(&["parent_of"]))
        .to_list();
    for monarch in &monarchs_with_kids {
        let name = get_name(&storage, monarch);
        // Count children for this monarch
        let child_count = g
            .v_ids([monarch.as_vertex_id().unwrap()])
            .out_labels(&["parent_of"])
            .count();
        println!("  {}: {} children", name, child_count);
    }

    // =========================================================================
    // SECTION 9: Transform Steps (Phase 7)
    // =========================================================================
    print_section("9. TRANSFORM STEPS (Phase 7: value_map, element_map, order, unfold)");

    // Query 28: Get person details using value_map()
    print_query("Get Elizabeth II's details using value_map()");
    let elizabeth_details = g
        .v()
        .has_value("name", "Elizabeth II")
        .value_map_keys(["name", "house", "reign_start", "reign_end"])
        .to_list();
    println!("Elizabeth II value_map:");
    for detail in &elizabeth_details {
        println!("  {:?}", detail);
    }

    // Query 29: Get complete element representation using element_map()
    print_query("Get Victoria's complete element_map()");
    let victoria_element = g
        .v()
        .has_value("name", "Victoria")
        .element_map_keys(["name", "house", "birth_date", "reign_start"])
        .to_list();
    println!("Victoria element_map:");
    for elem in &victoria_element {
        println!("  {:?}", elem);
    }

    // Query 30: Get marriage edge details using element_map()
    print_query("Get marriage edge details for Elizabeth II");
    let elizabeth_marriages = g
        .v()
        .has_value("name", "Elizabeth II")
        .out_e_labels(&["married_to"])
        .element_map()
        .to_list();
    println!("Elizabeth II's marriage edges (element_map):");
    for edge in &elizabeth_marriages {
        println!("  {:?}", edge);
    }

    // Query 31: Order monarchs by reign start date
    print_query("Monarchs ordered by reign start date (ascending)");
    let monarchs_by_reign = g
        .v()
        .has_label("person")
        .has_value("is_monarch", true)
        .has("reign_start") // Only those with reign_start
        .order()
        .by_key_asc("reign_start")
        .build()
        .to_list();
    println!("Monarchs by reign start:");
    for monarch in &monarchs_by_reign {
        if let Some(vid) = monarch.as_vertex_id() {
            if let Some(vertex) = storage.get_vertex(vid) {
                let name = vertex
                    .properties
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let reign = vertex
                    .properties
                    .get("reign_start")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                println!("  {} (reign started: {})", name, reign);
            }
        }
    }

    // Query 32: Order living royals alphabetically
    print_query("Living royals ordered alphabetically");
    let living_ordered = g
        .v()
        .has_label("person")
        .has_not("death_date")
        .order()
        .by_key_asc("name")
        .build()
        .limit(10)
        .to_list();
    println!("Living royals (first 10 alphabetically):");
    for person in &living_ordered {
        println!("  {}", get_name(&storage, person));
    }

    // Query 33: Unfold value_map entries
    print_query("Unfold Charles III's value_map into individual entries");
    let charles_props = g
        .v()
        .has_value("name", "Charles III")
        .value_map_keys(["name", "house", "reign_start"])
        .unfold()
        .to_list();
    println!("Charles III's properties (unfolded):");
    for prop in &charles_props {
        println!("  {:?}", prop);
    }

    // Query 34: Unfold titles list
    print_query("Get all titles held by Victoria (unfold list)");
    let victoria_titles = g
        .v()
        .has_value("name", "Victoria")
        .values("titles")
        .unfold()
        .to_list();
    println!("Victoria's titles:");
    for title in &victoria_titles {
        if let Value::String(t) = title {
            println!("  - {}", t);
        }
    }

    // =========================================================================
    // SECTION 10: Aggregation Steps (Phase 7)
    // =========================================================================
    print_section("10. AGGREGATION STEPS (Phase 7: group, group_count)");

    // Query 35: Group royals by house
    print_query("Group royals by house using group()");
    let by_house = g
        .v()
        .has_label("person")
        .group()
        .by_key("house")
        .by_value_key("name")
        .build()
        .to_list();
    println!("Royals grouped by house:");
    if let Some(Value::Map(map)) = by_house.first() {
        for (house, members) in map {
            if let Value::List(names) = members {
                println!("  {} ({} members): {:?}", house, names.len(), names);
            }
        }
    }

    // Query 36: Count royals by house using group_count()
    print_query("Count royals by house using group_count()");
    let house_counts = g
        .v()
        .has_label("person")
        .group_count()
        .by_key("house")
        .build()
        .to_list();
    println!("House counts:");
    if let Some(Value::Map(map)) = house_counts.first() {
        for (house, count) in map {
            println!("  {}: {}", house, format_value(count));
        }
    }

    // Query 37: Count royals by birth country
    print_query("Count royals by birth country using group_count()");
    let country_counts = g
        .v()
        .has_label("person")
        .has("birth_country") // Only those with birth_country
        .group_count()
        .by_key("birth_country")
        .build()
        .to_list();
    println!("Birth country counts:");
    if let Some(Value::Map(map)) = country_counts.first() {
        for (country, count) in map {
            println!("  {}: {}", country, format_value(count));
        }
    }

    // Query 38: Count edge types using group_count by label
    print_query("Count edge types using group_count().by_label()");
    let edge_counts = g.e().group_count().by_label().build().to_list();
    println!("Edge type counts:");
    if let Some(Value::Map(map)) = edge_counts.first() {
        for (label, count) in map {
            println!("  {}: {}", label, format_value(count));
        }
    }

    // Query 39: Group monarchs by whether they abdicated
    print_query("Group monarchs by abdication status");
    let monarchs_by_abdication = g
        .v()
        .has_label("person")
        .has_value("is_monarch", true)
        .group()
        .by_key("abdicated")
        .by_value_key("name")
        .build()
        .to_list();
    println!("Monarchs by abdication status:");
    if let Some(Value::Map(map)) = monarchs_by_abdication.first() {
        for (status, names) in map {
            println!("  abdicated={}: {:?}", status, names);
        }
    }

    // =========================================================================
    // Summary Statistics (Refactored with Phase 7 APIs)
    // =========================================================================
    print_section("SUMMARY STATISTICS (Using Phase 7 APIs)");

    println!("Total people: {}", vertex_count);

    // Monarchs count
    let total_monarchs = g.v().has_value("is_monarch", true).count();
    println!("Total monarchs: {}", total_monarchs);

    // Edge counts using group_count by label
    println!("\nRelationship counts (using group_count by label):");
    let edge_type_counts = g.e().group_count().by_label().build().to_list();
    if let Some(Value::Map(map)) = edge_type_counts.first() {
        for (label, count) in map {
            let count_val = match count {
                Value::Int(n) => *n,
                _ => 0,
            };
            // Marriages are bidirectional, so divide by 2
            if label == "married_to" {
                println!("  marriages: {}", count_val / 2);
            } else {
                println!("  {}: {}", label, count_val);
            }
        }
    }

    // House distribution using group_count
    println!("\nRoyals by house (using group_count):");
    let house_distribution = g
        .v()
        .has_label("person")
        .group_count()
        .by_key("house")
        .build()
        .to_list();
    if let Some(Value::Map(map)) = house_distribution.first() {
        // Sort for consistent output
        let mut houses: Vec<_> = map.iter().collect();
        houses.sort_by(|(a, _), (b, _)| a.cmp(b));
        for (house, count) in houses {
            println!("  {}: {}", house, format_value(count));
        }
    }

    // Monarchs ordered by reign using order()
    println!("\nMonarchs in chronological order (using order().by_key_asc()):");
    let monarchs_chronological = g
        .v()
        .has_label("person")
        .has_value("is_monarch", true)
        .has("reign_start")
        .order()
        .by_key_asc("reign_start")
        .build()
        .to_list();
    for monarch in &monarchs_chronological {
        if let Some(vid) = monarch.as_vertex_id() {
            if let Some(vertex) = storage.get_vertex(vid) {
                let name = vertex
                    .properties
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let reign_start = vertex
                    .properties
                    .get("reign_start")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let reign_end = vertex
                    .properties
                    .get("reign_end")
                    .and_then(|v| v.as_str())
                    .unwrap_or("present");
                println!("  {} ({} - {})", name, reign_start, reign_end);
            }
        }
    }

    // Living royals count
    let living_count = g.v().has_label("person").has_not("death_date").count();
    println!("\nLiving royals: {}", living_count);

    println!("\n=== Example Complete ===");
}
