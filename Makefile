#Run pipeline
run_r:
	python -m src.pipelines.mock_data_generator_v2
run_b:
	python -m src.pipelines.bronze_pipeline
run_s:
	python -m src.pipelines.silver_pipeline
run_g:
	python -m src.pipelines.gold_pipeline

container-streaming-up:
	docker compose up -d postgres kafka debezium flink-jobmanager flink-taskmanager
submit_flink:
	python -m src.pipelines.flink
generate_streaming_data:
	python -m src.utils.streaming_data