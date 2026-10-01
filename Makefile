#Run pipeline
run_b:
	python -m src.pipelines.bronze_pipeline
run_s:
	python -m src.pipelines.silver_pipeline
run_g:
	python -m src.pipelines.gold_pipeline