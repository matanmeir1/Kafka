from kafka import KafkaConsumer, KafkaProducer
import json
import time
from flask import Flask, request, jsonify
from kafka.errors import NoBrokersAvailable
import threading
import random
from datetime import datetime

app = Flask(__name__)

# In-memory database to store processed orders
orders_db = {}

# Dead Letter Queue Topic
DLQ_TOPIC = "orders_dlq"

# Function to create Kafka Producer for DLQ
def create_kafka_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            retry_backoff_ms=200
        )
    except Exception as e:
        print(f"ERROR: Failed to create Kafka producer for DLQ. Details: {e}")
        return None


def consume_orders():
    while True:
        try:
            print("DEBUG: Attempting to connect to Kafka for orders...")
            consumer = KafkaConsumer(
                'orders',
                bootstrap_servers=['kafka:9092'],
                group_id='order_consumer_group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest'
            )
            print("DEBUG: Connected to Kafka and subscribed to 'orders' topic.")
            for message in consumer:
                try:
                    print(f"DEBUG: Received message: {message.value}")
                    order = message.value
                    shipping_cost = round(order["totalAmount"] * 0.02, 2)
                    order["shippingCost"] = shipping_cost
                    orders_db[order["orderId"]] = order
                    print(f"DEBUG: Processed and stored order in orders_db: {order}")
                except KeyError as ke:
                    print(f"ERROR: Missing required fields in message. Routing to DLQ. Details: {ke}")
                    route_to_dlq(message.value)
                except Exception as e:
                    print(f"ERROR: Failed to process message. Routing to DLQ. Details: {e}")
                    route_to_dlq(message.value)
        except NoBrokersAvailable:
            print("ERROR: No Kafka brokers available. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"ERROR: Unexpected error in Kafka consumer. Details: {e}")
            time.sleep(5)


def route_to_dlq(message):
    """
    Sends the problematic message to the Dead Letter Queue (DLQ).
    """
    try:
        dlq_producer = create_kafka_producer()
        if dlq_producer:
            dlq_producer.send(DLQ_TOPIC, value=message)
            dlq_producer.flush()
            print(f"DEBUG: Message routed to DLQ: {message}")
        else:
            print("CRITICAL: Failed to create DLQ producer.")
    except Exception as e:
        print(f"CRITICAL: Failed to route message to DLQ. Details: {e}")


@app.route('/order-details', methods=['GET'])
def get_order_details():
    order_id = request.args.get('orderId')

    if not order_id or order_id not in orders_db:
        return jsonify({"error": "Order not found"}), 404

    return jsonify(orders_db[order_id])


@app.route('/getAllOrderIdsFromTopic', methods=['GET'])
def get_all_order_ids_from_topic():
    """Retrieve all unique order IDs from the 'orders' topic."""
    topic_name = request.args.get('topicName')

    if not topic_name:
        return jsonify({"error": "Missing topicName"}), 400

    try:
        random_consumer_group = f"temp_consumer_{random.randint(1, 100000)}"
        print(f"DEBUG: Using random consumer group '{random_consumer_group}'")

        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=['kafka:9092'],
            group_id=random_consumer_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        print(f"DEBUG: Connected to Kafka and subscribed to topic '{topic_name}'.")

        # Use a set to ensure uniqueness
        order_ids = set()
        for message in consumer:
            order_id = message.value.get("orderId")
            if order_id:
                order_ids.add(order_id)  # Add to the set (duplicates automatically removed)
                print(f"DEBUG: Collected orderId: {order_id}")

        consumer.close()
        return jsonify({"orderIds": list(order_ids)})  # Convert set to list for JSON serialization

    except Exception as e:
        print(f"ERROR: Failed to consume messages from topic '{topic_name}'. Details: {e}")
        return jsonify({"error": "Failed to consume messages", "details": str(e)}), 500


if __name__ == '__main__':
    orders_consumer_thread = threading.Thread(target=consume_orders)
    orders_consumer_thread.daemon = True
    orders_consumer_thread.start()

    app.run(host='0.0.0.0', port=5011)


#working version