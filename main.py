from data_generator.generate_data import run_simulation
from src.common.minio_client import MinioClient

def generate_data_to_minio():
    run_simulation(7)
    minio_client = MinioClient()
    minio_client.upload_directory("data_generator/output", "warehouse", "raw")
    
def main():
    generate_data_to_minio()
    
main()