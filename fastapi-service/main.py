from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, Column, String, Text, Date, DECIMAL, TIMESTAMP
from sqlalchemy.orm import sessionmaker, declarative_base
import requests
import dlt
from dlt.destinations import postgres
import os
from datetime import datetime

app = FastAPI()

# Database setup
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:password@postgres:5432/customer_db')
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class Customer(Base):
    __tablename__ = 'customers'
    __table_args__ = {'schema': 'customers'}

    customer_id = Column(String(50), primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    email = Column(String(255), nullable=False)
    phone = Column(String(20))
    address = Column(Text)
    date_of_birth = Column(Date)
    account_balance = Column(DECIMAL(15, 2))
    created_at = Column(TIMESTAMP)

# Create tables if not exist
Base.metadata.create_all(bind=engine)

@app.post("/api/ingest")
async def ingest():
    # Fetch all customers from Flask
    all_customers = []
    page = 1
    limit = 100
    flask_url = os.getenv('FLASK_URL', 'http://flask:5000')
    while True:
        try:
            resp = requests.get(f"{flask_url}/api/customers?page={page}&limit={limit}")
            resp.raise_for_status()
            data = resp.json()
            all_customers.extend(data['data'])
            if len(data['data']) < limit:
                break
            page += 1
        except requests.RequestException as e:
            raise HTTPException(status_code=500, detail=f"Failed to fetch from Flask: {str(e)}")

    # Use dlt to load into PostgreSQL
    pipeline = dlt.pipeline(
        pipeline_name='customer_pipeline',
        destination=postgres(credentials=DATABASE_URL),
        dataset_name='customers'
    )
    load_info = pipeline.run(
        all_customers,
        table_name='customers',
        write_disposition='merge',
        primary_key='customer_id'
    )
    return {"status": "success", "records_processed": len(all_customers)}

@app.get("/api/customers")
async def get_customers(page: int = Query(1, ge=1), limit: int = Query(10, ge=1)):
    offset = (page - 1) * limit
    with SessionLocal() as session:
        total = session.query(Customer).count()
        customers = session.query(Customer).offset(offset).limit(limit).all()
        data = []
        for customer in customers:
            customer_dict = customer.__dict__.copy()
            customer_dict.pop('_sa_instance_state', None)
            # Convert dates to string if needed, but FastAPI handles
            data.append(customer_dict)
    return {"data": data, "total": total, "page": page, "limit": limit}

@app.get("/api/customers/{customer_id}")
async def get_customer(customer_id: str):
    with SessionLocal() as session:
        customer = session.query(Customer).filter_by(customer_id=customer_id).first()
        if not customer:
            raise HTTPException(status_code=404, detail="Customer not found")
        customer_dict = customer.__dict__.copy()
        customer_dict.pop('_sa_instance_state', None)
        return customer_dict

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)