from kafka import KafkaProducer,KafkaConsumer
import time
import uuid
import json
from flask import Flask, render_template,request, jsonify
from collections import defaultdict

app = Flask(__name__)
driver_nodes = defaultdict(dict)  # Store registered driver nodes (Error Handling)
test_config = defaultdict(dict)
recent_tc=defaultdict(dict)
heartbeat=defaultdict(dict)
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

@app.route('/listener')
def listener():
    kafka_listener()
    return jsonify({"Listening"})

@app.route('/active-nodes')
def active_nodes():
    global driver_nodes
    global heartbeat
    #print(heartbeat)
    for i in heartbeat.keys():
        if i in driver_nodes.keys() and (time.time()-heartbeat[i])>5:
            #print(time.time()-heartbeat[i])
            driver_nodes.pop(i)
    return render_template('nodes.html',data=driver_nodes)
    
@app.route('/testconfig')
def testconfig():
    return render_template('testconfig.html')

@app.route('/post_testconfig',methods=['POST'])
def post_testconfig():
    global test_config
    global recent_tc
    test_id=str(uuid.uuid4())
    test_config[test_id]["test_id"]=test_id 
    test_config[test_id]["test_type"]=request.form['test_type']
    test_config[test_id]["test_message_delay"]=request.form['test_message_delay']
    if(test_config[test_id]["test_type"]=="AVALANCHE"):
        test_config[test_id]["test_message_delay"]=0
    test_config[test_id]["message_count_per_driver"]=request.form['message_count_per_driver']
    recent_tc=test_config[test_id]
    print(test_config)
    try:
        kafka_producer.send(topic2,test_config[test_id])
    except Exception as e:
        print(e)
    return '<h1>Test configurations uploaded successfully</h1>'

@app.route('/trigger')
def trigger():
    global recent_tc
    return render_template('trigger.html',data=recent_tc)
    
@app.route('/send_trigger',methods=['POST'])
def send_trigger():
    global test_config
    trigger_msg=defaultdict(dict)
    trigger_msg["test_id"]=test_config["test_id"]
    trigger_msg["trigger"]="YES"
    try:
        kafka_producer.send(topic3,trigger_msg)
    except Exception as e:
        print(e)
    return '<h1> Trigger raised successfully</h1'

@app.route('/dashboard')
def dashboard():
    global driver_nodes
    return render_template('metrics.html',data=driver_nodes)

def register_node(dict):
    global driver_nodes
    node_id=dict["node_id"]
    for i in driver_nodes.values():
        if i["node_ip"]==dict["node_ip"]:
            return
    driver_nodes[node_id]["node_ip"]=dict['node_ip']   # To store ip address
    driver_nodes[node_id]["test_config"] = {}   # Store test configuration
    driver_nodes[node_id]["metrics"] = {}

def update_metrics(dict):
    global driver_nodes
    global test_config
    node_id=dict["node_id"]
    test_id=dict["test_id"]
    driver_nodes[node_id]["test_config"]=test_config[test_id]
    driver_nodes[node_id]["metrics"]=dict["metrics"]

def handle_heartbeats(dict):
    global heartbeat
    node_id=dict["node_id"]
    if dict["heartbeat"]=="YES":
        heartbeat[node_id]=dict["timestamp"]
     
def kafka_listener():
    for message in kafka_consumer:
        if message.topic == topic1:
            #print("Hello")
            if(isinstance(message.value,str)):
                node_dict=json.loads(message.value)
                register_node(node_dict)
            else:
                register_node(message.value)
        if message.topic==topic4:
            #print("Hi")
            print(message.value)
            update_metrics(message.value) 
        if message.topic == topic5:
            if(isinstance(message.value,str)):
                handle_heartbeats(json.loads(message.value))
            else:
                handle_heartbeats(message.value)       
    
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)  # Start the Flask app