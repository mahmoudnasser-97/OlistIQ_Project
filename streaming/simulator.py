import json
import random
import time
import uuid
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# CONFIGURATION

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "olist_orders_stream"
SLEEP_BETWEEN_EVENTS = 2

fake = Faker("pt_BR")

# REFERENCE DATA

PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]

# Realistic Brazilian e-commerce payment distribution
# credit_card dominates, boleto second, voucher third, debit_card least
PAYMENT_TYPE_WEIGHTS = [45, 30, 15, 10]

ORDER_STATUSES = [
    "created", "approved", "processing",
    "shipped", "delivered", "canceled", "unavailable"
]
ORDER_STATUS_WEIGHTS = [2, 5, 5, 15, 65, 6, 2]

PRODUCT_CATEGORIES_PT = [
    "beleza_saude", "informatica_acessorios", "automotivo",
    "cama_mesa_banho", "moveis_decoracao", "esporte_lazer",
    "perfumaria", "utilidades_domesticas", "telefonia",
    "watches_gifts", "alimentos_bebidas", "bebes",
    "moda_calcados_acessorios", "eletronicos", "ferramentas_jardim",
    "papelaria", "brinquedos", "livros_interesse_geral",
    "construcao_ferramentas_seguranca", "fashion_bolsas_e_acessorios"
]

# Weighted so popular categories appear more: matches real Olist distribution
PRODUCT_CATEGORY_WEIGHTS = [
    12, 10, 8, 9, 7, 8, 6, 7, 6,
    4, 5, 4, 5, 5, 3, 2, 3, 2, 2, 2
]

# Translation map: Portuguese to English
CATEGORY_TRANSLATION = {
    "beleza_saude": "Beauty & Health",
    "informatica_acessorios": "Computers & Accessories",
    "automotivo": "Automotive",
    "cama_mesa_banho": "Bed, Bath & Table",
    "moveis_decoracao": "Furniture & Decor",
    "esporte_lazer": "Sports & Leisure",
    "perfumaria": "Perfumery",
    "utilidades_domesticas": "Home Utilities",
    "telefonia": "Telephony",
    "watches_gifts": "Watches & Gifts",
    "alimentos_bebidas": "Food & Beverages",
    "bebes": "Baby Products",
    "moda_calcados_acessorios": "Fashion & Accessories",
    "eletronicos": "Electronics",
    "ferramentas_jardim": "Garden Tools",
    "papelaria": "Stationery",
    "brinquedos": "Toys",
    "livros_interesse_geral": "Books",
    "construcao_ferramentas_seguranca": "Construction & Safety",
    "fashion_bolsas_e_acessorios": "Bags & Accessories"
}

BRAZILIAN_STATES = [
    "SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO",
    "ES", "PE", "CE", "PA", "MT", "MS", "RN", "PB",
    "AM", "AL", "SE", "PI", "RO", "TO", "AC", "AP", "MA", "DF"
]

# Weighted to reflect real Brazilian e-commerce geography
# SP dominates, followed by RJ, MG, RS, PR
BRAZILIAN_STATE_WEIGHTS = [
    25, 12, 10, 7, 7, 6, 5, 4,
    3, 3, 3, 2, 2, 2, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1
]


# GENERATOR FUNCTIONS

def generate_customer():
    """
    Mirrors: olist_customers_dataset.csv
    Fields: customer_id, customer_unique_id, customer_zip_code_prefix,
            customer_city, customer_state
    """
    state = random.choices(BRAZILIAN_STATES, weights=BRAZILIAN_STATE_WEIGHTS, k=1)[0]
    return {
        "customer_id": str(uuid.uuid4()),
        "customer_unique_id": str(uuid.uuid4()),
        "customer_zip_code_prefix": str(random.randint(10000, 99999)),
        "customer_city": fake.city(),
        "customer_state": state
    }


def generate_seller():
    """
    Mirrors: olist_sellers_dataset.csv
    Fields: seller_id, seller_zip_code_prefix, seller_city, seller_state
    """
    state = random.choices(BRAZILIAN_STATES, weights=BRAZILIAN_STATE_WEIGHTS, k=1)[0]
    return {
        "seller_id": str(uuid.uuid4()),
        "seller_zip_code_prefix": str(random.randint(10000, 99999)),
        "seller_city": fake.city(),
        "seller_state": state
    }


def generate_product():
    """
    Mirrors: olist_products_dataset.csv
    Fields: product_id, product_category_name, product_category_name_english,
            product_name_length, product_description_length, product_photos_qty,
            product_weight_g, product_length_cm, product_height_cm, product_width_cm
    """
    category_pt = random.choices(PRODUCT_CATEGORIES_PT, weights=PRODUCT_CATEGORY_WEIGHTS, k=1)[0]
    category_en = CATEGORY_TRANSLATION[category_pt]
    return {
        "product_id": str(uuid.uuid4()),
        "product_category_name": category_pt,
        "product_category_name_english": category_en,
        "product_name_length": random.randint(20, 60),
        "product_description_length": random.randint(100, 3000),
        "product_photos_qty": random.choices(
            [1, 2, 3, 4, 5, 6],
            weights=[30, 25, 20, 12, 8, 5],
            k=1
        )[0],
        "product_weight_g": random.choices(
            list(range(100, 30001, 100)),
            weights=None,
            k=1
        )[0],
        "product_length_cm": random.randint(10, 100),
        "product_height_cm": random.randint(5, 50),
        "product_width_cm": random.randint(10, 100)
    }


def generate_order(customer_id):
    """
    Mirrors: olist_orders_dataset.csv
    Fields: order_id, customer_id, order_status, order_purchase_timestamp,
            order_approved_at, order_delivered_carrier_date,
            order_delivered_customer_date, order_estimated_delivery_date
    """
    purchase_time = datetime.now()
    approved_time = purchase_time + timedelta(minutes=random.randint(5, 60))
    carrier_time = approved_time + timedelta(days=random.randint(1, 5))
    delivered_time = carrier_time + timedelta(days=random.randint(1, 10))
    estimated_time = purchase_time + timedelta(days=random.randint(10, 40))

    status = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1)[0]

    # Compute delivery time in days for analytics
    delivery_days = (delivered_time - purchase_time).days

    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "order_status": status,
        "order_purchase_timestamp": purchase_time.isoformat(),
        "order_approved_at": approved_time.isoformat(),
        "order_delivered_carrier_date": carrier_time.isoformat(),
        "order_delivered_customer_date": delivered_time.isoformat(),
        "order_estimated_delivery_date": estimated_time.isoformat(),
        "delivery_days": delivery_days
    }


def generate_order_item(order_id, seller_id, product_id):
    """
    Mirrors: olist_order_items_dataset.csv
    Fields: order_id, order_item_id, product_id, seller_id,
            shipping_limit_date, price, freight_value
    """
    price = round(random.uniform(10.0, 800.0), 2)
    freight = round(random.uniform(5.0, 80.0), 2)
    shipping_limit = datetime.now() + timedelta(days=random.randint(3, 15))

    return {
        "order_id": order_id,
        "order_item_id": 1,
        "product_id": product_id,
        "seller_id": seller_id,
        "shipping_limit_date": shipping_limit.isoformat(),
        "price": price,
        "freight_value": freight
    }


def generate_payment(order_id):
    """
    Mirrors: olist_order_payments_dataset.csv
    Fields: order_id, payment_sequential, payment_type,
            payment_installments, payment_value
    """
    payment_type = random.choices(PAYMENT_TYPES, weights=PAYMENT_TYPE_WEIGHTS, k=1)[0]
    installments = 1 if payment_type != "credit_card" else random.choices(
        [1, 2, 3, 4, 6, 8, 10, 12],
        weights=[20, 18, 16, 14, 12, 8, 6, 6],
        k=1
    )[0]
    value = round(random.uniform(20.0, 1000.0), 2)

    return {
        "order_id": order_id,
        "payment_sequential": 1,
        "payment_type": payment_type,
        "payment_installments": installments,
        "payment_value": value
    }


def generate_review(order_id):
    """
    Mirrors: olist_order_reviews_dataset.csv
    Fields: review_id, order_id, review_score, review_comment_title,
            review_comment_message, review_creation_date,
            review_answer_timestamp
    """
    score = random.choices([1, 2, 3, 4, 5], weights=[5, 5, 10, 20, 60], k=1)[0]
    creation = datetime.now()
    answer = creation + timedelta(days=random.randint(1, 7))
    has_comment = random.random() > 0.4

    return {
        "review_id": str(uuid.uuid4()),
        "order_id": order_id,
        "review_score": score,
        "review_comment_title": fake.sentence(nb_words=4) if random.random() > 0.5 else "",
        "review_comment_message": fake.sentence(nb_words=12) if has_comment else "",
        "review_has_comment": has_comment,
        "review_creation_date": creation.isoformat(),
        "review_answer_timestamp": answer.isoformat()
    }


def generate_full_event():
    """
    Combines all sub-generators into one self-contained order event.
    """
    customer = generate_customer()
    seller = generate_seller()
    product = generate_product()
    order = generate_order(customer["customer_id"])
    order_item = generate_order_item(order["order_id"], seller["seller_id"], product["product_id"])
    payment = generate_payment(order["order_id"])
    review = generate_review(order["order_id"])

    return {
        "event_timestamp": datetime.now().isoformat(),
        "order": order,
        "customer": customer,
        "seller": seller,
        "product": product,
        "order_item": order_item,
        "payment": payment,
        "review": review
    }


# KAFKA PRODUCER

def create_producer():
    """
    Tries to connect to Kafka. Retries every 5 seconds if not ready.
    """
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8")
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready yet, retrying in 5 seconds...")
            time.sleep(5)


def run_simulator():
    producer = create_producer()
    event_count = 0

    print(f"Starting Olist event simulator connecting to topic: {KAFKA_TOPIC}")
    print(f"Sending one event every {SLEEP_BETWEEN_EVENTS} seconds. Press Ctrl+C to stop\n")

    while True:
        try:
            event = generate_full_event()
            order_id = event["order"]["order_id"]

            producer.send(
                topic=KAFKA_TOPIC,
                key=order_id,
                value=event
            )
            producer.flush()

            event_count += 1
            print(
                f"[{event_count}] Sent order {order_id[:8]}... | "
                f"Status: {event['order']['order_status']} | "
                f"Payment: {event['payment']['payment_type']} | "
                f"Installments: {event['payment']['payment_installments']} | "
                f"Score: {event['review']['review_score']}⭐ | "
                f"Category: {event['product']['product_category_name_english']} | "
                f"Value: R${event['payment']['payment_value']}"
            )

            time.sleep(SLEEP_BETWEEN_EVENTS)

        except KeyboardInterrupt:
            print("\nSimulator stopped by user")
            producer.close()
            break


if __name__ == "__main__":
    run_simulator()