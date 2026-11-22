// Read-only dashboard endpoints served by the controller. The controller
// gathers worker stats inside the cluster network; the browser never talks
// to workers.
import { getJSON } from "./http";
import type { DashboardOverview, EventsResponse } from "./types";

export function getOverview(signal?: AbortSignal): Promise<DashboardOverview> {
  return getJSON<DashboardOverview>("/dashboard/overview", signal);
}

export function getEvents(after: number, signal?: AbortSignal): Promise<EventsResponse> {
  return getJSON<EventsResponse>(`/dashboard/events?after=${after}`, signal);
}

export async function controllerHealthy(signal?: AbortSignal): Promise<boolean> {
  try {
    const res = await fetch("/healthz", { signal });
    return res.ok;
  } catch {
    return false;
  }
}
