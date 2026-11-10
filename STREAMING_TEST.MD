# Read message from topic
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic pg_server.public.sku_inventory \
    --from-beginning \
    --max-messages 1

#