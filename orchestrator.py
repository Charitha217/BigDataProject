from kafka import KafkaProducer,KafkaConsumer
import sys
import json
from flask import Flask, request, jsonify
from threading import Thread
from collections import defaultdict

app = Flask(__name__)
driver_nodes = defaultdict(dict)  # Store registered driver nodes (Error Handling)

# Get Kafka and Orchestrator IP Addresses from command-line arguments
#kafka_ip = sys.argv[1]
#orchestrator_ip = sys.argv[2]

# Kafka topics
topic1 = 'register'
topic2 = 'test_config'
topic3 = 'trigger'
topic4 = 'metrics'
topic5 = 'heartbeat'

# Kafka producer for sending commands to nodes
kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
# Kafka consumer
kafka_consumer = KafkaConsumer(topic1, topic2, topic3, topic4, topic5, bootstrap_servers='localhost:9092', api_version=(0, 11, 5), value_deserializer=lambda x: json.loads(x.decode('utf-8')))

'''def send_command_to_nodes(command):
    # Send the command to the orchestrator commands topic
    kafka_producer.send('orchestrator_commands_topic', json.dumps({"command": command}))

# Example: Send a command to all nodes
send_command_to_nodes("SomeCommand")'''

@app.route('/')
def welc_pg():
    return 'Welcome to Distributed Load Testing System'

@app.route('/active-nodes')
def active_nodes():
    global driver_nodes
    kafka_listener()
    print(driver_nodes)
    return jsonify({"active_nodes": list(driver_nodes)})

def register_node(node_id, node_ip):
    global driver_nodes
    driver_nodes[node_id]['node_ip']=node_ip      # To store ip address
    driver_nodes[node_id]['test_config'] = {}    # Store test configuration
    driver_nodes[node_id]['metrics'] = {}

def kafka_listener():
    global driver_nodes
    print("Hello",driver_nodes)
    for message in kafka_consumer:
        print("Check1")
        if message.topic == topic1:
            if(isinstance(message.value,str)):
                node_dict=json.loads(message.value)
                register_node(node_dict['node_id'], node_dict['node_ip'])
                break
        print(driver_nodes)

if __name__ == '__main__':
    
    app.run(host='127.0.0.1', port=5000, debug=True)  # Start the Flask app
    