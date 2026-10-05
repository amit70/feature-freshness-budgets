import pandas as pd

df = pd.read_csv("matrix_multi_results.csv")

print("=== Frontier: error rate, mean request latency (T1), mean features dropped (T2) vs multiplier ===")
summary = df.groupby(["arm", "decision_window_multiplier"]).agg(
    error_rate=("error", "mean"),
    fn_rate=("false_negative", "mean"),
    mean_request_latency=("request_latency", "mean"),
    mean_n_dropped=("n_dropped", "mean"),
    timeout_rate=("timed_out", "mean"),
).reset_index()
summary.to_csv("matrix_multi_summary.csv", index=False)

for arm in ["B0", "T1", "T2"]:
    print(f"\n--- {arm} ---")
    sub = summary[summary.arm == arm].sort_values("decision_window_multiplier")
    print(sub.drop(columns="arm").round(4).to_string(index=False))

print("\n=== T2 collapse check: multiplier where mean_n_dropped approaches n_features (5) ===")
t2 = summary[summary.arm == "T2"]
print(t2[["decision_window_multiplier", "mean_n_dropped", "error_rate"]].round(3).to_string(index=False))
