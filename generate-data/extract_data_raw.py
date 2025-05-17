import duckdb
from faker import Faker
import pandas as pd
import random
import uuid
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import os
import time

# Medir tempo de geração
start_gen = time.time()

fake = Faker('pt_BR')
Faker.seed(42)
random.seed(42)

# Configurações
num_users = 10000
min_events = 5
max_events = 15
event_types = ["login", "view_product", "add_to_cart", "begin_checkout", "purchase"]

def write_with_row_group_size(table, root_path, partition_cols, row_group_size=128*1024):
    partitions = table.to_pandas().groupby(partition_cols)
    for keys, subdf in partitions:
        subdf_sorted = subdf.sort_values(by=['user_id', 'event_name'])
        sub_table = pa.Table.from_pandas(subdf_sorted)
        sub_path = root_path
        for col, val in zip(partition_cols, keys):
            sub_path = os.path.join(sub_path, f"{col}={val}")
        os.makedirs(sub_path, exist_ok=True)
        file_path = os.path.join(sub_path, "part-0.parquet")
        pq.write_table(sub_table, file_path, compression='zstd', row_group_size=row_group_size)

def generate_user(user_id):
    birth_date = fake.date_of_birth(minimum_age=18, maximum_age=65)
    age = (datetime.now().date() - birth_date).days // 365
    latitude = float(fake.latitude())
    longitude = float(fake.longitude())
    return {
        "user_id": user_id,
        "name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "birth_date": birth_date,
        "age": age,
        "gender": random.choice(["male", "female", "non_binary"]),
        "marital_status": random.choice(["single", "married", "divorced"]),
        "education": random.choice(["high_school", "bachelor", "master", "phd"]),
        "employment_status": random.choice(["employed", "unemployed", "student", "retired"]),
        "income": round(random.uniform(1500, 15000), 2),
        "country": "Brasil",
        "state": fake.estado_sigla(),
        "city": fake.city(),
        "neighborhood": fake.bairro(),
        "zipcode": fake.postcode(),
        "latitude": latitude,
        "longitude": longitude,
        "timezone": fake.timezone(),
        "is_logged_in": False,
        "has_newsletter": random.choice([True, False]),
        "login_method": random.choice(["email", "google", "facebook"]),
        "has_payment_method": random.choice([True, False]),
        "fidelity_program": random.choice([True, False]),
    }

def generate_event(user, session_id, last_event_time):
    product_name = fake.word().capitalize()
    product_price = round(random.uniform(20, 500), 2)
    discount = round(random.uniform(0, 0.5), 2)
    final_price = round(product_price * (1 - discount), 2)
    product_id = str(uuid.uuid4())
    product = {
        "product_id": product_id,
        "product_name": product_name,
        "product_category": random.choice(["eletrônicos", "livros", "moda", "casa", "esportes"]),
        "product_subcategory": fake.word(),
        "brand": fake.company(),
        "price_original": product_price,
        "discount": discount,
        "price_final": final_price,
        "stock_qty": random.randint(0, 1000),
        "available": random.choice([True, False]),
        "release_date": fake.date_this_decade(),
    }

    timestamp = last_event_time + timedelta(minutes=random.randint(1, 10))
    event_name = random.choice(event_types)

    return {
        **user,
        **product,
        "event_id": str(uuid.uuid4()),
        "event_name": event_name,
        "event_timestamp": timestamp,
        "session_id": session_id,
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "os": random.choice(["iOS", "Android", "Windows", "MacOS", "Linux"]),
        "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
        "platform": random.choice(["web", "app"]),
        "screen_resolution": random.choice(["1920x1080", "1366x768", "1440x900", "1280x720"]),
        "language": random.choice(["pt-BR", "en-US", "es-ES"]),
        "connection_type": random.choice(["wifi", "4g", "5g", "cable"]),
        "page_depth": random.randint(1, 10),
        "scroll_depth_percent": round(random.uniform(10, 100), 2),
        "products_viewed": [fake.word() for _ in range(random.randint(1, 5))],
        "cart_value_total": final_price if event_name in ["add_to_cart", "begin_checkout", "purchase"] else 0.0,
        "cart_items_count": random.randint(1, 3) if event_name in ["add_to_cart", "begin_checkout", "purchase"] else 0,
        "wishlist_items": [fake.word() for _ in range(random.randint(0, 2))],
        "product_interaction_type": random.choice(["view", "hover", "zoom", "compare"]),
        "avg_time_per_page": round(random.uniform(5, 60), 2),
        "clicks_per_session": random.randint(1, 30),
        "interaction_score": round(random.uniform(0.1, 1.0), 2),
        "gclid": str(uuid.uuid4())[:10],
        "fbclid": str(uuid.uuid4())[:10],
        "campaign_type": random.choice(["awareness", "conversion", "retargeting"]),
        "ad_group_name": fake.word(),
        "ad_creative_id": str(uuid.uuid4())[:8],
        "session_start": last_event_time,
        "session_end": timestamp,
        "session_duration_sec": (timestamp - last_event_time).seconds,
    }

# Geração dos dados
data = []
for i in range(num_users):
    user_id = str(uuid.uuid4())
    user = generate_user(user_id)
    session_id = str(uuid.uuid4())
    last_event_time = datetime.now() - timedelta(days=random.randint(1, 60))
    num_events = random.randint(min_events, max_events)
    for _ in range(num_events):
        event = generate_event(user, session_id, last_event_time)
        data.append(event)
        last_event_time = event["event_timestamp"]

# Fim da geração
end_gen = time.time()
print(f"Tempo de geração dos dados: {end_gen - start_gen:.2f} segundos")

# Convertendo para Arrow
df = pd.DataFrame(data)
arrow_table = pa.Table.from_pandas(df)

# Tempo de início de salvamento
start_save = time.time()

# Diretório e colunas de partição
output_dir = 'output_sem_funnel'
partition_columns = ['country', 'state', 'device']
write_with_row_group_size(arrow_table, output_dir, partition_columns)

# Fim do salvamento
end_save = time.time()
print(f"Tempo para salvar os dados: {end_save - start_save:.2f} segundos")
