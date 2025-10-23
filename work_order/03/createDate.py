import random
import string
import pandas as pd
from datetime import datetime, timedelta

# 随机生成字符串
def random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# 随机生成日期
def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

# 模拟数据生成
def generate_data(rows=10000):
    data = []
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 1, 31)
    
    for _ in range(rows):
        user_id = random.randint(100000, 999999)
        product_id = random.randint(1000, 9999)
        sku_id = random.randint(100, 999)
        visit_time = random_date(start_date, end_date)
        action = random.choice(['visit', 'collect', 'add_to_cart', 'purchase'])
        price = round(random.uniform(10, 1000), 2)
        channel = random.choice(['direct', 'search', 'social', 'email', 'ad'])
        user_type = random.choice(['price_sensitive', 'impulse_buyer', 'casual'])
        rating = random.randint(1, 5)
        
        data.append([
            user_id, product_id, sku_id, visit_time, action, price, channel, user_type, rating
        ])
    
    columns = ['user_id', 'product_id', 'sku_id', 'visit_time', 'action', 'price', 'channel', 'user_type', 'rating']
    df = pd.DataFrame(data, columns=columns)
    return df

# 生成数据并保存到CSV文件
df = generate_data(10000)
df.to_csv('ecommerce_data.csv', index=False)