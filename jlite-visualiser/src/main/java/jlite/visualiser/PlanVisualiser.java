package jlite.visualiser;

/**
 * Execution plan visualiser.
 *
 * Renders a physical query plan as an interactive tree in the browser.
 *
 * TODO: EXPLAIN command returns plan as JSON tree.
 * TODO: React/SVG web renderer:
 *   - each node shows operator, estimated rows, actual rows, cost, time.
 *   - hot-path highlighting (nodes exceeding X% of total cost).
 *   - index usage badge (green when index scan used).
 *   - diff view: before/after ANALYZE or query rewrite.
 * TODO: CLI command \visualise <sql> opens browser tab.
 */
public class PlanVisualiser {
    // TODO: implement
}
