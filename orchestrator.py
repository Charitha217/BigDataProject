from kafka import KafkaProducer,KafkaConsumer
import sys
import uuid
import json
from flask import Flask, render_template,request, jsonify
from threading import Thread
from collections import defaultdict

app = Flask(__name__)
driver_nodes = defaultdict(dict)  # Store registered driver nodes (Error Handling)
test_config = defaultdict(dict)
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

@app.route('/')
def welc_pg():
    return 'Welcome to Distributed Load Testing System'

@app.route('/active-nodes')
def active_nodes():
    global driver_nodes
    kafka_listener()
    print(driver_nodes)
    return jsonify({"active_nodes": list(driver_nodes)})

@app.route('/testconfig')
def testconfig():
    return render_template('testconfig.html')

@app.route('/post_testconfig',methods=['POST'])
def post_testconfig():
    global test_config
    testid = str(uuid.uuid4())
    test_config['test_id']= testid,
    test_config['test_type']=request.form['test_type']
    test_config['test_message_delay']=request.form['test_message_delay']
    if(test_config['test_type']=="AVALANCHE"):
        test_config['test_message_delay']=0
    test_config['message_count_per_driver']=request.form['message_count_per_driver']
    print(test_config)
    try:
        kafka_producer.send(topic2,test_config)
    except Exception as e:
        print(e)
    return '<h1>Test configurations uploaded successfully</h1>'

@app.route('/trigger')
def trigger():
    global test_config
    return render_template('trigger.html',data=test_config)
    
@app.route('/send_trigger',methods=['POST'])
def send_trigger():
    global test_config
    trigger_msg=defaultdict(dict)
    trigger_msg['test_id']=test_config['test_id']
    trigger_msg['trigger']="YES"
    try:
        kafka_producer.send(topic3,trigger_msg)
    except Exception as e:
        print(e)
    return '<h1> Trigger raised successfully</h1'

def register_node(node_id, node_ip):
    global driver_nodes
    driver_nodes[node_id]['node_ip']=node_ip      # To store ip address
    driver_nodes[node_id]['test_config'] = {}    # Store test configuration
    driver_nodes[node_id]['metrics'] = {}

def kafka_listener():
    global driver_nodes
    for message in kafka_consumer:
        if message.topic == topic1:
            if(isinstance(message.value,str)):
                node_dict=json.loads(message.value)
                register_node(node_dict['node_id'], node_dict['node_ip'])
                break
        print(driver_nodes)

if __name__ == '__main__':
    
    app.run(host='127.0.0.1', port=5000, debug=True)  # Start the Flask app
    