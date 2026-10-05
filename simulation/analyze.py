import pandas as pd

df = pd.read_csv("matrix_results.csv")

group_cols = ["arm", "ingestion_lag", "aggregation_window", "decision_window", "burst_count"]

summary = df.groupby(group_cols).agg(
    n=("error", "size"),
    error_rate=("error", "mean"),
    fn_rate=("false_negative", "mean"),
    fp_rate=("false_positive", "mean"),
    drop_rate=("dropped", "mean"),
    mean_age=("age", "mean"),
    mean_added_latency=("added_latency", "mean"),
    timeout_rate=("timed_out", "mean"),
).reset_index()

summary.to_csv("matrix_summary.csv", index=False)

# Headline: mean error/drop/latency rate by arm, averaged across all cells
print("=== Overall by arm (averaged across all swept cells) ===")
overall = df.groupby("arm").agg(
    error_rate=("error", "mean"),
    fn_rate=("false_negative", "mean"),
    drop_rate=("dropped", "mean"),
    mean_age=("age", "mean"),
    mean_added_latency=("added_latency", "mean"),
    timeout_rate=("timed_out", "mean"),
)
print(overall.round(4))

print("\n=== Error rate vs decision_window, tight cases only (agg=5, burst=12) ===")
tight = df[(df.aggregation_window == 5.0) & (df.burst_count == 12)]
pivot = tight.groupby(["decision_window", "arm"])["error"].mean().unstack()
print(pivot.round(4))

print("\n=== T2 completeness-cost curve: drop_rate and error_rate vs decision_window (all lags) ===")
t2 = summary[summary.arm == "T2"]
t2_pivot = t2.groupby("decision_window")[["drop_rate", "error_rate"]].mean()
print(t2_pivot.round(4))

print("\n=== T1 latency cost: mean_added_latency vs decision_window ===")
t1 = summary[summary.arm == "T1"]
t1_pivot = t1.groupby("decision_window")[["mean_added_latency", "timeout_rate", "error_rate"]].mean()
print(t1_pivot.round(4))
