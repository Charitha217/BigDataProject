from kafka import KafkaProducer, KafkaConsumer
import time
import random
import statistics
import requests
import uuid
import sys
import json

topic1 = 'register'
topic2 = 'test_config'
topic3 = 'trigger'
topic4 = 'metrics'
topic5 = 'heartbeat'

# Get Kafka and Orchestrator IP Addresses from command-line arguments
kafka_ip = sys.argv[1]
orchestrator_ip = sys.argv[2]

# Kafka producer for registration
kafka_producer = KafkaProducer(bootstrap_servers=f'{kafka_ip}:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Kafka consumer for commands
kafka_consumer = KafkaConsumer('orchestrator_commands_topic', bootstrap_servers=f'{kafka_ip}:9092', api_version=(0, 11, 5), value_deserializer=lambda x: json.loads(x.decode('utf-8')))

'''def process_commands():
    # Subscribe to orchestrator commands
    for message in command_consumer:
        # Process the received command
        command = json.loads(message.value)
        print(f"Node {node_id} received command: {command}")


# Process commands from the orchestrator
process_commands()
'''

if __name__ == '__main__':
    # Unique ID for the node
    node_id = str(uuid.uuid4())
    register_node={
        "node_id": node_id,
        "node_ip": "http://127.0.0.1:5010",
        "message_type": "DRIVER_NODE_REGISTER"
    }
    try:
        print(register_node)
        kafka_producer.send(topic1,json.dumps(register_node))
    except Exception as e:
        print(str(e))
    kafka_producer.flush()
    kafka_producer.close()