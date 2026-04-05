from flask import Flask, request, jsonify
import json
import os

app = Flask(__name__)

# Load customer data from JSON file
data_path = os.path.join(os.path.dirname(__file__), 'data', 'customers.json')
with open(data_path) as f:
    customers = json.load(f)

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"})

@app.route('/api/customers', methods=['GET'])
def get_customers():
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 10))
    start = (page - 1) * limit
    end = start + limit
    data = customers[start:end]
    total = len(customers)
    return jsonify({
        "data": data,
        "total": total,
        "page": page,
        "limit": limit
    })

@app.route('/api/customers/<customer_id>', methods=['GET'])
def get_customer(customer_id):
    customer = next((c for c in customers if c['customer_id'] == customer_id), None)
    if not customer:
        return jsonify({"error": "Customer not found"}), 404
    return jsonify(customer)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)