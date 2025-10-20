# save as generate_mysql_data.py
import random
import pymysql
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("zh_CN")
FARM_SHOPS = [f"shop_{i:03d}" for i in range(1, 21)]  # 20 家店铺
PRODUCT_CNT = 500  # 商品数量
PRODUCTS = [f"prod_{i:05d}" for i in range(1, PRODUCT_CNT+1)]
USERS = [f"user_{random.randint(1000000,9999999)}" for _ in range(4000)]  # 4k users

# MySQL connection - 请替换为你的配置
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "password"
MYSQL_DB = "testdb"

conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
                       password=MYSQL_PASSWORD, database=MYSQL_DB, charset='utf8mb4')
cur = conn.cursor()

# helper: random datetime in range (过去30天)
def rand_dt_within(days=30):
    start = datetime.now() - timedelta(days=days)
    delta = random.random() * (days * 24 * 3600)
    return start + timedelta(seconds=delta)

# 1) generate ods_logs (pv)
pv_rows = []
PV_COUNT = 20000
for i in range(PV_COUNT):
    shop_no = random.choice(FARM_SHOPS)
    user = random.choice(USERS)
    prod = random.choice(PRODUCTS)
    dt = rand_dt_within(30)
    duration = max(0, int(random.gauss(60, 80)))  # 平均 60s，含噪声
    is_bounce = 1 if duration < 5 and random.random() < 0.6 else 0
    ds = dt.date()
    pv_rows.append((shop_no, user, prod, dt.strftime("%Y-%m-%d %H:%M:%S"), duration, is_bounce, fake.url(), ds.strftime("%Y-%m-%d")))

# batch insert
cur.executemany("""INSERT INTO ods_logs (shop_no,user_id,product_id,event_time,duration_seconds,is_bounce,referer,ds)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", pv_rows)
conn.commit()
print("Inserted ods_logs:", len(pv_rows))

# 2) dim_favorite (de-duplicated by user+product+ds)
fav_rows = []
FAV_COUNT = 5000
seen = set()
while len(fav_rows) < FAV_COUNT:
    user = random.choice(USERS)
    prod = random.choice(PRODUCTS)
    shop = random.choice(FARM_SHOPS)
    dt = rand_dt_within(30)
    ds = dt.date().strftime("%Y-%m-%d")
    key = (user, prod, ds)
    if key in seen:
        continue
    seen.add(key)
    fav_rows.append((user, prod, shop, dt.strftime("%Y-%m-%d %H:%M:%S"), ds))
cur.executemany("""INSERT INTO dim_favorite (user_id,product_id,shop_no,favor_time,ds)
                  VALUES (%s,%s,%s,%s,%s)""", fav_rows)
conn.commit()
print("Inserted dim_favorite:", len(fav_rows))

# 3) dim_cart
cart_rows = []
CART_COUNT = 4000
for i in range(CART_COUNT):
    user = random.choice(USERS)
    prod = random.choice(PRODUCTS)
    shop = random.choice(FARM_SHOPS)
    qty = random.randint(1,5)
    dt = rand_dt_within(30)
    ds = dt.date().strftime("%Y-%m-%d")
    cart_rows.append((user, prod, shop, qty, dt.strftime("%Y-%m-%d %H:%M:%S"), ds))
cur.executemany("""INSERT INTO dim_cart (user_id,product_id,shop_no,qty,add_time,ds)
                  VALUES (%s,%s,%s,%s,%s,%s)""", cart_rows)
conn.commit()
print("Inserted dim_cart:", len(cart_rows))

# 4) order_info + order_detail
order_rows = []
detail_rows = []
ORDER_COUNT = 2000
for i in range(ORDER_COUNT):
    user = random.choice(USERS)
    shop = random.choice(FARM_SHOPS)
    order_time_dt = rand_dt_within(30)
    status = random.choices(["CREATED","PAID","CANCELLED"], weights=[0.4,0.5,0.1])[0]
    pay_time = None
    total_amount = 0.0
    if status == "PAID":
        pay_time_dt = order_time_dt + timedelta(minutes=random.randint(1,120))
        pay_time = pay_time_dt.strftime("%Y-%m-%d %H:%M:%S")
    order_no = f"ON{int(datetime.timestamp(order_time_dt))}{random.randint(100,999)}"
    ds = order_time_dt.date().strftime("%Y-%m-%d")
    order_rows.append((order_no, user, shop, order_time_dt.strftime("%Y-%m-%d %H:%M:%S"), pay_time, status, 0.00, ds))
# insert order_info and get generated ids
cur.executemany("""INSERT INTO order_info (order_no,user_id,shop_no,order_time,pay_time,status,total_amount,ds)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", order_rows)
conn.commit()
print("Inserted order_info:", len(order_rows))

# fetch inserted order ids and times to build details
cur.execute("SELECT id, order_time, status FROM order_info ORDER BY id DESC LIMIT %s", (ORDER_COUNT,))
rows = cur.fetchall()
# rows returned in reverse order because ORDER BY id DESC, let's re-query by id asc for convenience
cur.execute("SELECT id, order_time, status FROM order_info ORDER BY id ASC LIMIT %s", (ORDER_COUNT,))
rows = cur.fetchall()
for idx, order_time, status in rows:
    items = random.randint(1,3)
    for j in range(items):
        prod = random.choice(PRODUCTS)
        qty = random.randint(1,4)
        unit_price = round(random.uniform(10,500),2)
        amount = round(unit_price * qty,2)
        detail_rows.append((idx, prod, random.choice(FARM_SHOPS), random.choice(['red','blue','black']), random.choice(['S','M','L']), qty, unit_price, amount))
        total_amount += amount
    # update order total_amount
    cur.execute("UPDATE order_info SET total_amount=%s WHERE id=%s", (round(total_amount,2), idx))
conn.commit()

# insert order_detail
cur.executemany("""INSERT INTO order_detail (order_id,product_id,shop_no,sku_color,sku_size,qty,unit_price,amount)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", detail_rows)
conn.commit()
print("Inserted order_detail:", len(detail_rows))

cur.close()
conn.close()
print("Data generation completed.")
