// Categorical series colors for the dark theme, in fixed order (validated
// reference palette, dark steps). A worker keeps its color for the whole
// session: color follows the entity, never its rank.
export const SERIES = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#0ca30c", "#9085e9", "#e66767"];
export const OTHER = "#898781";

const assigned = new Map<string, number>();

export function workerColor(workerId: string): string {
  let slot = assigned.get(workerId);
  if (slot === undefined) {
    slot = assigned.size;
    assigned.set(workerId, slot);
  }
  return slot < SERIES.length ? SERIES[slot] : OTHER;
}
