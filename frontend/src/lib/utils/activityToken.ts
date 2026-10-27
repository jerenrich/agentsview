export type ActivityKind = "model" | "tool" | "unattributed";

export function activityToken(kind: ActivityKind): string {
  const tokens: Record<ActivityKind, string> = {
    model: "var(--cat-task)",
    tool: "var(--cat-tool)",
    unattributed: "var(--cat-other)",
  };
  return tokens[kind];
}
