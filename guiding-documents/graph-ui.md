# Graph Navigation UI Specification

**Status**: Draft  
**Dependencies**: web-ui.md, Phase 3 (Traversal Engine)  
**Purpose**: Define interactive graph exploration patterns for the Interstellar Web UI

---

## 1. Overview

This document specifies the interactive navigation patterns for exploring graph data visually. These patterns complement the query interface by providing intuitive, point-and-click exploration suitable for both developers debugging traversals and non-technical users discovering data relationships.

### 1.1 Design Principles

1. **Progressive disclosure**: Start simple, reveal complexity on demand
2. **Reversible actions**: Every navigation action can be undone
3. **Context preservation**: Never lose sight of where you came from
4. **Performance-aware**: Gracefully handle large graphs via lazy loading

---

## 2. Navigation Patterns

### 2.1 Node-Centric Exploration (Click-to-Explore)

The primary interaction model: click nodes to reveal their neighborhood.

**Interactions:**
| Action | Behavior |
|--------|----------|
| Single click | Select node, show properties panel |
| Double click | Expand immediate neighbors (configurable: out/in/both) |
| Shift+click | Add to multi-selection |
| Right click | Context menu with advanced options |

**Context Menu Options:**
```
┌─────────────────────────────┐
│ Expand outgoing edges    →  │
│ Expand incoming edges    →  │
│ Expand all edges            │
├─────────────────────────────┤
│ Expand by label...       →  │
│   ├─ knows                  │
│   ├─ works_at               │
│   └─ created                │
├─────────────────────────────┤
│ Hide this node              │
│ Hide unconnected nodes      │
│ Focus on this node          │
├─────────────────────────────┤
│ Find paths to...            │
│ Pin to sidebar              │
│ Copy ID                     │
└─────────────────────────────┘
```

**Expand Behavior:**
```
Before click:                    After double-click on Alice:

    ┌───────┐                        ┌───────┐
    │ Alice │                        │ Alice │
    └───────┘                        └───┬───┘
                                         │ knows
                          ┌──────────────┼──────────────┐
                          ▼              ▼              ▼
                     ┌───────┐      ┌───────┐      ┌───────┐
                     │  Bob  │      │ Carol │      │ David │
                     └───────┘      └───────┘      └───────┘
```

---

### 2.2 Breadcrumb Trail Navigation

Track exploration history as a clickable path.

**UI Component:**
```
┌─────────────────────────────────────────────────────────────────────────┐
│ Trail: [Alice] → knows → [Bob] → works_at → [Acme Corp] → employs → ...│
│                                                                    [×]  │
└─────────────────────────────────────────────────────────────────────────┘
```

**Behavior:**
- Each bracketed item is a clickable node that re-centers the view
- Arrows show the edge label used to navigate
- Trail auto-truncates with "..." when too long
- [×] clears the trail and resets view

**Implementation:**
```
Trail State:
[
  { type: "node", id: 1, label: "person", name: "Alice" },
  { type: "edge", label: "knows" },
  { type: "node", id: 2, label: "person", name: "Bob" },
  { type: "edge", label: "works_at" },
  { type: "node", id: 100, label: "company", name: "Acme Corp" }
]
```

---

### 2.3 Semantic Zoom Levels

Different detail levels based on zoom factor.

**Level 1 - Cluster View (Zoomed Out):**
```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│         ┌─────────────┐                                     │
│         │   person    │                                     │
│         │   (150)     │─────────────┐                       │
│         └─────────────┘             │                       │
│               │                     │                       │
│               │ knows (320)         │ works_at (150)        │
│               │                     │                       │
│               ▼                     ▼                       │
│         ┌─────────────┐       ┌─────────────┐               │
│         │   person    │       │   company   │               │
│         │   (150)     │       │    (45)     │               │
│         └─────────────┘       └─────────────┘               │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Level 2 - Node View (Mid Zoom):**
```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│    ┌───────┐         ┌───────┐         ┌───────┐           │
│    │ Alice │─────────│  Bob  │─────────│ Carol │           │
│    └───────┘  knows  └───────┘  knows  └───────┘           │
│        │                 │                                  │
│        │ works_at        │ works_at                         │
│        │                 │                                  │
│        ▼                 ▼                                  │
│    ┌────────┐        ┌────────┐                             │
│    │  Acme  │        │TechCo  │                             │
│    └────────┘        └────────┘                             │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Level 3 - Detail View (Zoomed In):**
```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│              ┌─────────────────────┐                        │
│              │ person: Alice       │                        │
│              ├─────────────────────┤                        │
│              │ age: 30             │                        │
│              │ email: alice@ex.com │                        │
│              │ city: Seattle       │                        │
│              └──────────┬──────────┘                        │
│                         │                                   │
│                         │ knows (since: 2020)               │
│                         ▼                                   │
│              ┌─────────────────────┐                        │
│              │ person: Bob         │                        │
│              ├─────────────────────┤                        │
│              │ age: 25             │                        │
│              │ email: bob@ex.com   │                        │
│              └─────────────────────┘                        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Zoom Thresholds:**
| Zoom Level | Threshold | Display Mode |
|------------|-----------|--------------|
| Cluster | < 0.3 | Labels as clusters with counts |
| Node | 0.3 - 0.7 | Individual nodes, label only |
| Detail | > 0.7 | Nodes with inline properties |

---

### 2.4 Filtered Neighborhood Views

Control which relationships are visible during exploration.

**Filter Panel:**
```
┌─────────────────────────────────┐
│ Edge Filters                    │
├─────────────────────────────────┤
│ [✓] knows (320)                 │
│ [✓] works_at (150)              │
│ [ ] created (89)                │
│ [ ] reviewed (45)               │
├─────────────────────────────────┤
│ Direction                       │
│ (•) Outgoing  ( ) Incoming      │
│ ( ) Both                        │
├─────────────────────────────────┤
│ Expansion Depth                 │
│ [1] [2] [3] [All]               │
├─────────────────────────────────┤
│ Property Filters                │
│ age: [18] to [65]               │
│ city: [________▼]               │
└─────────────────────────────────┘
```

**Filtered Expansion Example:**
```
Filter: only "knows" edges, depth=2

                              ┌───────┐
                              │ Alice │ ← Start node
                              └───┬───┘
                                  │ knows (depth 1)
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
               ┌───────┐    ┌───────┐    ┌───────┐
               │  Bob  │    │ Carol │    │ David │
               └───┬───┘    └───┬───┘    └───────┘
                   │            │
                   │ knows      │ knows (depth 2)
                   ▼            ▼
              ┌───────┐    ┌───────┐
              │  Eve  │    │ Frank │
              └───────┘    └───────┘

Note: David has no outgoing "knows" edges, so no expansion
      works_at edges are hidden due to filter
```

---

### 2.5 Path-Based Navigation

Find and visualize paths between nodes.

**Path Finder Dialog:**
```
┌─────────────────────────────────────────────────────────────┐
│ Find Paths                                             [×]  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│ From: [Alice_____________▼]  To: [Acme Corp_________▼]     │
│                                                             │
│ Max hops: [3____]   Edge filter: [All edges________▼]      │
│                                                             │
│ [ ] Shortest only   [✓] Avoid cycles                       │
│                                                             │
│                              [Find Paths]                   │
└─────────────────────────────────────────────────────────────┘
```

**Path Results Visualization:**
```
Found 2 paths from Alice to Acme Corp:

Path 1 (2 hops) ─────────────────────────────────
┌───────┐  works_at   ┌──────────┐
│ Alice │────────────▶│ Acme Corp│
└───────┘             └──────────┘

Path 2 (3 hops) ─────────────────────────────────
┌───────┐  knows   ┌───────┐  works_at   ┌──────────┐
│ Alice │─────────▶│  Bob  │────────────▶│ Acme Corp│
└───────┘          └───────┘             └──────────┘

[Show Path 1] [Show Path 2] [Show All] [Animate]
```

**Animated Path Traversal:**
```
Step 1/3:                    Step 2/3:                    Step 3/3:

  ┌───────┐                    ┌───────┐                    ┌───────┐
  │►Alice │                    │ Alice │                    │ Alice │
  └───────┘                    └───┬───┘                    └───┬───┘
      │                            │►knows                     │ knows
      │                            ▼                           ▼
      ▼                        ┌───────┐                   ┌───────┐
  ┌───────┐                    │► Bob  │                   │  Bob  │
  │  Bob  │                    └───────┘                   └───┬───┘
  └───────┘                        │                           │►works_at
      │                            │                           ▼
      ▼                            ▼                       ┌──────────┐
  ┌──────────┐                 ┌──────────┐                │►Acme Corp│
  │ Acme Corp│                 │ Acme Corp│                └──────────┘
  └──────────┘                 └──────────┘

  [◀ Prev] [▶ Next]  Speed: [Slow] [Med] [Fast]
```

---

### 2.6 Search-Driven Navigation

Jump directly to nodes matching search criteria.

**Search Bar:**
```
┌─────────────────────────────────────────────────────────────┐
│ 🔍 Search: [alice_______________________________________]   │
├─────────────────────────────────────────────────────────────┤
│ Suggestions:                                                │
│   ┌─────────────────────────────────────────────────────┐  │
│   │ ● person: Alice          name="Alice", age=30       │  │
│   │ ○ person: Alicia         name="Alicia", age=28      │  │
│   │ ○ company: Alice Springs  name="Alice Springs"      │  │
│   └─────────────────────────────────────────────────────┘  │
│                                                             │
│ Search in: (•) All  ( ) Labels  ( ) Properties             │
└─────────────────────────────────────────────────────────────┘
```

**Advanced Search Panel:**
```
┌─────────────────────────────────────────────────────────────┐
│ Advanced Search                                        [×]  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│ Label: [person___________▼]                                │
│                                                             │
│ Properties:                                                 │
│ ┌────────────┬────────────┬────────────────────┐           │
│ │ Key        │ Operator   │ Value              │           │
│ ├────────────┼────────────┼────────────────────┤           │
│ │ age        │ >=         │ 25                 │           │
│ │ city       │ equals     │ Seattle            │           │
│ │ [+ Add]    │            │                    │           │
│ └────────────┴────────────┴────────────────────┘           │
│                                                             │
│ Results limit: [100___]                                     │
│                                                             │
│                              [Search] [Clear]               │
└─────────────────────────────────────────────────────────────┘
```

**Search Results:**
```
┌─────────────────────────────────────────────────────────────┐
│ Search Results: 15 matches                                  │
├─────────────────────────────────────────────────────────────┤
│ ┌─────┬────────┬─────────────────────────────────────────┐ │
│ │     │ Label  │ Properties                              │ │
│ ├─────┼────────┼─────────────────────────────────────────┤ │
│ │ [→] │ person │ Alice, age=30, city=Seattle             │ │
│ │ [→] │ person │ Bob, age=25, city=Seattle               │ │
│ │ [→] │ person │ Carol, age=28, city=Seattle             │ │
│ │ ... │        │                                         │ │
│ └─────┴────────┴─────────────────────────────────────────┘ │
│                                                             │
│ [→] = Navigate to node   [Show All on Graph]               │
└─────────────────────────────────────────────────────────────┘
```

---

### 2.7 Layout Modes

User-selectable graph layouts via Cytoscape.js.

**Layout Selector:**
```
┌─────────────────────────────────────────┐
│ Layout: [Force-Directed_________▼]      │
│         ├─ Force-Directed (default)     │
│         ├─ Hierarchical (top-down)      │
│         ├─ Hierarchical (left-right)    │
│         ├─ Circular                     │
│         ├─ Grid                         │
│         └─ Concentric                   │
└─────────────────────────────────────────┘
```

**Layout Examples:**

**Force-Directed (Social Networks):**
```
              ┌───────┐
              │ Alice │
              └───┬───┘
        ┌─────────┼─────────┐
        ▼         ▼         ▼
   ┌───────┐ ┌───────┐ ┌───────┐
   │  Bob  │ │ Carol │ │ David │
   └───┬───┘ └───────┘ └───┬───┘
       │                   │
       └───────┬───────────┘
               ▼
          ┌───────┐
          │  Eve  │
          └───────┘
```

**Hierarchical (Org Charts, Taxonomies):**
```
                    ┌───────────┐
                    │   CEO     │
                    └─────┬─────┘
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
       ┌─────────┐   ┌─────────┐   ┌─────────┐
       │   CTO   │   │   CFO   │   │   COO   │
       └────┬────┘   └────┬────┘   └────┬────┘
            │             │             │
       ┌────┴────┐        │        ┌────┴────┐
       ▼         ▼        ▼        ▼         ▼
   ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐
   │Dev 1 │ │Dev 2 │ │Acct 1│ │Ops 1 │ │Ops 2 │
   └──────┘ └──────┘ └──────┘ └──────┘ └──────┘
```

**Circular (Hub Detection):**
```
                    ┌───────┐
                    │  Bob  │
                   ╱└───────┘╲
                  ╱           ╲
            ┌───────┐     ┌───────┐
            │ Carol │     │ David │
            └───────┘     └───────┘
               │    ╲   ╱    │
               │     ╲ ╱     │
               │  ┌───────┐  │
               └──│ Alice │──┘   ← Central hub
                  └───────┘
               ╱      │      ╲
              ╱       │       ╲
        ┌───────┐ ┌───────┐ ┌───────┐
        │  Eve  │ │ Frank │ │ Grace │
        └───────┘ └───────┘ └───────┘
```

**Grid (Comparison View):**
```
┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐
│ Alice │ │  Bob  │ │ Carol │ │ David │
│ age:30│ │ age:25│ │ age:28│ │ age:35│
└───────┘ └───────┘ └───────┘ └───────┘

┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐
│  Eve  │ │ Frank │ │ Grace │ │ Henry │
│ age:22│ │ age:40│ │ age:33│ │ age:29│
└───────┘ └───────┘ └───────┘ └───────┘
```

---

### 2.8 Mini-Map and Viewport

Navigation aid for large graphs.

**Main View with Mini-Map:**
```
┌─────────────────────────────────────────────────────────────┬──────────┐
│                                                             │ ┌──────┐ │
│                                                             │ │ ·· · │ │
│              ┌───────┐                                      │ │·[  ]·│ │
│              │ Alice │                                      │ │ · ·· │ │
│              └───┬───┘                                      │ │  · · │ │
│        ┌─────────┼─────────┐                                │ └──────┘ │
│        ▼         ▼         ▼                                │  Mini-map│
│   ┌───────┐ ┌───────┐ ┌───────┐                            │          │
│   │  Bob  │ │ Carol │ │ David │   ← Current viewport       │ [  ] =   │
│   └───────┘ └───────┘ └───────┘                            │ viewport │
│                                                             │          │
│   (more nodes outside viewport...)                          │          │
│                                                             │          │
└─────────────────────────────────────────────────────────────┴──────────┘

Mini-map interactions:
- Click anywhere to jump to that region
- Drag the viewport rectangle to pan
- Shows full graph extent at all times
```

---

### 2.9 History and Undo

Navigate through exploration states.

**History Controls:**
```
┌─────────────────────────────────────────────────────────────┐
│ [◀ Back] [▶ Forward] [↺ Reset]           History: 5/12     │
└─────────────────────────────────────────────────────────────┘
```

**History Panel (Expanded):**
```
┌─────────────────────────────────────────────────────────────┐
│ Exploration History                                    [×]  │
├─────────────────────────────────────────────────────────────┤
│ 12. Expanded David → 2 nodes                     [Restore] │
│ 11. Filtered to "knows" edges only               [Restore] │
│ 10. Expanded Carol → 3 nodes                     [Restore] │
│  9. Expanded Bob → 1 node                        [Restore] │
│ ─────────────────────────────────────────────────────────── │
│ ► 8. Expanded Alice → 3 nodes (current)                    │ 
│ ─────────────────────────────────────────────────────────── │
│  7. Searched for "Alice"                         [Restore] │
│  6. Changed layout to Force-Directed             [Restore] │
│  5. Loaded query results                         [Restore] │
│ ...                                                         │
└─────────────────────────────────────────────────────────────┘
```

**State Snapshot Contents:**
```javascript
{
  timestamp: "2024-01-15T10:30:00Z",
  action: "expand_node",
  description: "Expanded Alice → 3 nodes",
  state: {
    visible_nodes: [1, 2, 3, 4],
    visible_edges: [101, 102, 103],
    positions: { 1: {x: 100, y: 200}, ... },
    zoom: 0.8,
    pan: { x: 50, y: 30 },
    selected: [1],
    filters: { edge_labels: ["knows", "works_at"] }
  }
}
```

---

## 3. Pinned Nodes Sidebar

Keep important nodes accessible during exploration.

**Sidebar:**
```
┌─────────────────────┐
│ Pinned Nodes    [▼] │
├─────────────────────┤
│ ★ Alice             │
│   person, age=30    │
│   [Go] [Unpin]      │
├─────────────────────┤
│ ★ Acme Corp         │
│   company           │
│   [Go] [Unpin]      │
├─────────────────────┤
│ ★ Project X         │
│   software          │
│   [Go] [Unpin]      │
├─────────────────────┤
│                     │
│ Drag nodes here     │
│ to pin them         │
│                     │
└─────────────────────┘
```

**Interactions:**
- Drag any node onto sidebar to pin
- [Go] centers view on that node
- [Unpin] removes from sidebar
- Pinned nodes highlighted in graph view

---

## 4. Full UI Layout

**Complete Interface:**
```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Interstellar          [◀][▶][↺]    🔍 [_______________]    [Stats][Schema][?]│
├─────────────────────────────────────────────────────────────────────────────┤
│ Trail: [Alice] → knows → [Bob] → works_at → [Acme Corp]                [×] │
├───────────────┬─────────────────────────────────────────────┬───────────────┤
│ Pinned     [▼]│                                             │ Filters    [▼]│
├───────────────┤                                             ├───────────────┤
│ ★ Alice       │                                             │ Edge Labels   │
│   [Go][Unpin] │         ┌───────┐                           │ [✓] knows     │
├───────────────┤         │ Alice │                           │ [✓] works_at  │
│ ★ Acme Corp   │         └───┬───┘                           │ [ ] created   │
│   [Go][Unpin] │    ┌────────┼────────┐                      ├───────────────┤
├───────────────┤    ▼        ▼        ▼                      │ Direction     │
│               │┌──────┐ ┌──────┐ ┌──────┐                   │ (•) Out ( ) In│
│               ││ Bob  │ │Carol │ │David │                   ├───────────────┤
│               │└──────┘ └──────┘ └──────┘                   │ Depth         │
│               │                                             │ [1][2][3][All]│
│               │                                             ├───────────────┤
│               │                                  ┌────────┐ │ Layout        │
│               │                                  │ ·[  ]· │ │ [Force______▼]│
│               │                                  │  · · · │ │               │
│               │                                  └────────┘ │               │
├───────────────┴─────────────────────────────────────────────┴───────────────┤
│ Properties: Alice                                                           │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │ Label: person  │  name: "Alice"  │  age: 30  │  city: "Seattle"        │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Space` | Toggle selected node expansion |
| `Enter` | Center view on selected node |
| `Delete` | Hide selected nodes |
| `Escape` | Clear selection |
| `Ctrl+Z` | Undo last action |
| `Ctrl+Shift+Z` | Redo |
| `Ctrl+F` | Focus search box |
| `Ctrl+A` | Select all visible nodes |
| `+` / `-` | Zoom in / out |
| `0` | Reset zoom |
| `1-4` | Switch layout modes |
| `?` | Show keyboard shortcuts |

---

## 6. Implementation Notes

### 6.1 Performance Considerations

- **Lazy loading**: Only fetch node details when expanded
- **Viewport culling**: Don't render nodes outside visible area
- **Edge bundling**: Collapse parallel edges at low zoom
- **Progressive rendering**: Show structure first, details later
- **Max visible nodes**: Warn user above 500 nodes, force clustering above 2000

### 6.2 State Management

```javascript
// Core UI state
const graphUIState = {
  // Visible elements
  nodes: Map<NodeId, NodeState>,
  edges: Map<EdgeId, EdgeState>,
  
  // View state
  zoom: number,
  pan: { x: number, y: number },
  layout: 'force' | 'hierarchical' | 'circular' | 'grid',
  
  // Selection
  selected: Set<NodeId>,
  hovered: NodeId | null,
  
  // Filters
  visibleEdgeLabels: Set<string>,
  direction: 'out' | 'in' | 'both',
  expansionDepth: number,
  
  // Navigation
  trail: TrailEntry[],
  history: HistoryEntry[],
  historyIndex: number,
  pinnedNodes: Set<NodeId>,
};
```

### 6.3 API Endpoints Required

```
GET  /api/graph/node/:id/neighbors?direction=out&labels=knows,works_at&depth=1
GET  /api/graph/node/:id/details
GET  /api/graph/paths?from=1&to=100&maxHops=5
GET  /api/graph/search?q=alice&labels=person&limit=20
GET  /api/graph/cluster?zoomLevel=0.2
```

---

## 7. Future Enhancements

- **Collaborative mode**: Share exploration sessions via URL
- **Annotations**: Add notes to nodes during exploration
- **Snapshots**: Save and restore named view states
- **Comparison view**: Side-by-side node neighborhoods
- **Time-based filtering**: For graphs with temporal edges
- **Custom node styling**: Color/size by property values
- **Export**: Save current view as PNG/SVG

---

## 8. References

- [web-ui.md](./web-ui.md) - Parent Web UI specification
- [Cytoscape.js Documentation](https://js.cytoscape.org/)
- [htmx Documentation](https://htmx.org/docs/)
