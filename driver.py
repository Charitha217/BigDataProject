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
#active_nodes_list=[]

# Kafka producer for registration
kafka_producer = KafkaProducer(bootstrap_servers=f'{kafka_ip}:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Kafka consumer for commands
kafka_consumer = KafkaConsumer(topic1,topic2,topic3,topic4,topic5, bootstrap_servers=f'{kafka_ip}:9092', api_version=(0, 11, 5), value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def send_request(target_server_url):
    start_time = time.time()
    response = requests.get(target_server_url) # Sending GET request to target server
    end_time = time.time()
    latency = (end_time - start_time) * 1000  # Convert to milliseconds
    return latency

def send_metrics(node_id,test_config,latencies):
    msg={}
    msg["node_id"]=node_id
    msg["test_id"]=test_config["test_id"]
    msg["report_id"]=str(uuid.uuid4())
    msg["metrics"]={}
    msg["metrics"]["mean_latency"]=statistics.mean(latencies)
    msg["metrics"]["median_latency"]=statistics.median(latencies)
    msg["metrics"]["min_latency"]=min(latencies)
    msg["metrics"]["max_latency"]=max(latencies)
    kafka_producer.send(topic1,msg)
    print(msg)
    kafka_producer.flush()

def avalanche_test(node_id,test_config,target_server_url):
    latencies = []
    request_count = 0
    count=int(test_config["message_count_per_driver"])
    while request_count<count:
        latency = send_request(target_server_url)
        latencies.append(latency)
        send_metrics(node_id,test_config,latencies)
        request_count += 1
        
def tsunami_test(node_id,test_config,target_server_url):
    latencies = []
    request_count = 0
    delay=int(test_config["test_message_delay"])
    count=int(test_config["message_count_per_driver"])
    while request_count<count:
        latency = send_request(target_server_url)
        latencies.append(latency)
        send_metrics(node_id,test_config,latencies)
        request_count += 1
        time.sleep(delay)
    

def start_test(node_id,test_config,target_server_url):
    if(test_config["test_type"]=='AVALANCHE'):
        avalanche_test(node_id,test_config,target_server_url)
    else:
        tsunami_test(node_id,test_config,target_server_url)

def kafka_listener(node_id):
    active_nodes_list=[]
    test_config={}
    trig={}
    for message in kafka_consumer:
        if message.topic == topic2:
            test_config=message.value
            print(test_config)
        if message.topic == topic3:
            trig=message.value
            print(trig)
            if(trig["trigger"]=="YES"):
                    start_test(node_id,test_config,'http://127.0.0.1:8000/ping')

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
    
    kafka_listener(node_id)