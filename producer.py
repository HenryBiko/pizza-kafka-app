import streamlit as st
import uuid
import json
from confluent_kafka import Producer
from dotenv import load_dotenv
import os

load_dotenv()

st.set_page_config(page_title="Pizza Order System", layout="centered")


@st.cache_resource
def get_producer():

    producer_config = {
        # "bootstrap.servers": "localhost:9092"
    'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv("SASL_USERNAME"),
        'sasl.password': os.getenv("SASL_PASSWORD")

    }
    return Producer(producer_config)
producer= get_producer()
st.title("🍕 Henry's Pizza Portal")
st.markdown("---")

# 2. Add a simple check to see if the script is running
#st.write("UI Status: 🟢 Loading Form...")

def delivery_report(err, msg):
    if err:
        print('Message failed delivery: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.value().decode("utf-8"), msg.partition()))
        print(dir(msg))
        print(f"Delivered message: {msg.topic()}: partition  {msg.partition()}: at offset {msg.offset()}")
# Start of form
with st.form("pizza_form"):
    st.write("Step 2: Inside the Form... 🟢")

    col1, col2 = st.columns(2)

    with col1:
        user_name = st.text_input("👤 Customer Name")
        pizza_type = st.selectbox("🍕 Select Pizza", ["Pepperoni", "Cheese", "Hawaiian", "Meat Lovers"])

    with col2:
        quantity = st.number_input("🔢 Quantity", min_value=1, max_value=20, value=1)
        crust = st.radio("🍞 Crust Type", ["Thin", "Hand-Tossed", "Stuffed"])

    # Submit button
    submitted = st.form_submit_button("🚀 Place Order")
    if submitted:
        if user_name:
            # 1. Create the data object correctly
            order_data = {
                "order_id": str(uuid.uuid4()),
                "user": user_name,
                "item": pizza_type,
                "quantity": quantity,
                "crust": crust
            }

    # Checkpoint 2
        st.info("Step 3: Script Finished ✅")

        value = json.dumps(order_data).encode('utf-8')
        producer.produce('Orders', value=value, callback=delivery_report)
        producer.flush()
        st.success(f"Order placed for {user_name}!")
