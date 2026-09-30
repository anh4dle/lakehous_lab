#Run pipeline
rtb:
	python -m src.pipelines.raw_to_bronze
rtb_order:
	python -m src.pipelines.raw_to_bronze_order
bts:
	python -m src.pipelines.bronze_to_silver
bts_order:
	python -m src.pipelines.bronze_to_silver_order
stg:
	python -m src.pipelines.silver_to_gold