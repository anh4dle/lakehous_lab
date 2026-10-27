#Run pipeline
run_r:
	python -m src.pipelines.mock_data_generator_v2
run_b:
	python -m src.pipelines.bronze_pipeline
run_s:
	python -m src.pipelines.silver_pipeline
run_g:
	python -m src.pipelines.gold_pipeline