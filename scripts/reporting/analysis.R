View(read_feather(file.path(results_path, "trade_history.feather")))
View(read_feather(file.path(results_path, "submitted_trades.feather")))
View(read_csv(file.path(results_path, "runtime_log"), col_names=F))
