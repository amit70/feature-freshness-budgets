import pandas as pd

df = pd.read_csv("fine_matrix_multi_results.csv")

summary = df.groupby(["arm", "decision_window_multiplier"]).agg(
    error_rate=("error", "mean"),
    mean_n_dropped=("n_dropped", "mean"),
    mean_request_latency=("request_latency", "mean"),
).reset_index()
summary.to_csv("fine_matrix_multi_summary.csv", index=False)

M_MIN = 0.5012   # min_f(m*_f): merchant_diversity_1hr -- predicted error-recovery point
M_MAX = 0.6008   # max_f(m*_f): geo_velocity_10min -- predicted full-completeness point

t2 = summary[summary.arm == "T2"].sort_values("decision_window_multiplier")
print("=== T2: error_rate and mean_n_dropped across the fine band ===")
print(t2.drop(columns="arm").round(4).to_string(index=False))

print(f"\nPredicted min_f(m*_f) = {M_MIN}  (error should start dropping here)")
print(f"Predicted max_f(m*_f) = {M_MAX}  (mean_n_dropped should approach a residual floor here)")
