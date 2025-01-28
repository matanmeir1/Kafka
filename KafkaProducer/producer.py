from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.errors import KafkaError
import random
import json
from datetime import datetime

app = Flask(__name__)

DLQ_TOPIC = "orders_dlq"  # Dead Letter Queue topic

# Function to create a Kafka Producer with error handling

def create_kafka_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,  # Enable retries
            retry_backoff_ms=200  # Backoff time between retries
        )
    except Exception as e:
        print(f"ERROR: Failed to create Kafka producer. Details: {e}")
        return None


@app.route('/create-order', methods=['POST'])
def create_order():
    data = request.json
    order_id = data.get('orderId')
    num_items = data.get('itemsNum')

    if not order_id or not isinstance(num_items, int) or num_items <= 0:
        return jsonify({"error": "Invalid input"}), 400

    items = [
        {
            "itemId": f"ITEM-{i+1}",
            "quantity": random.randint(1, 5),
            "price": round(random.uniform(10, 100), 2)
        }
        for i in range(num_items)
    ]
    total_amount = sum(item["quantity"] * item["price"] for item in items)

    statuses = ["new", "processing", "shipped", "cancelled"]
    random_status = random.choice(statuses)

    order = {
        "orderId": order_id,
        "customerId": f"CUST-{random.randint(1, 1000)}",
        "orderDate": datetime.now().isoformat(),
        "items": items,
        "totalAmount": round(total_amount, 2),
        "currency": "USD",
        "status": random_status
    }

    producer = create_kafka_producer()
    if not producer:
        return jsonify({"error": "Failed to create Kafka producer"}), 500

    try:
        producer.send('orders', value=order).get(timeout=10)  # Block to catch errors
        producer.flush()
        print(f"DEBUG: Order sent to Kafka: {order}")
    except KafkaError as e:
        print(f"ERROR: Failed to publish message to Kafka. Routing to DLQ. Details: {e}")
        try:
            producer.send(DLQ_TOPIC, value=order)
            producer.flush()
        except Exception as dlq_error:
            print(f"CRITICAL: Failed to route to DLQ. Details: {dlq_error}")
        return jsonify({"error": "Failed to publish message", "details": str(e)}), 500
    finally:
        producer.close()

    return jsonify(order), 201


@app.route('/update-order', methods=['PUT'])
def update_order():
    data = request.json
    order_id = data.get('orderId')
    new_status = data.get('status')

    if not order_id or not new_status:
        return jsonify({"error": "Missing orderId or status"}), 400

    update_message = {
        "orderId": order_id,
        "updatedStatus": new_status,
        "updateTimestamp": datetime.now().isoformat()
    }

    producer = create_kafka_producer()
    if not producer:
        return jsonify({"error": "Failed to create Kafka producer"}), 500

    try:
        producer.send('orders', value=update_message).get(timeout=10)
        producer.flush()
        print(f"DEBUG: Update message sent to Kafka: {update_message}")
    except KafkaError as e:
        print(f"ERROR: Failed to publish update message to Kafka. Routing to DLQ. Details: {e}")
        try:
            producer.send(DLQ_TOPIC, value=update_message)
            producer.flush()
        except Exception as dlq_error:
            print(f"CRITICAL: Failed to route to DLQ. Details: {dlq_error}")
        return jsonify({"error": "Failed to publish update", "details": str(e)}), 500
    finally:
        producer.close()

    return jsonify({"message": "Order update request sent successfully", "details": update_message}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5010)

#working version