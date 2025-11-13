import os
import time
import random
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ============================
# 1. 读取 .env 文件参数
# ============================
load_dotenv()

QWEN_API_KEY = os.getenv("QWEN_API_KEY")
QWEN_API_BASE = os.getenv("QWEN_API_BASE")
QWEN_MODEL = os.getenv("QWEN_MODEL")

DB_SQLSERVER_HOST = os.getenv("DB_SQLSERVER_HOST")
DB_SQLSERVER_PORT = os.getenv("DB_SQLSERVER_PORT")
DB_SQLSERVER_USER = os.getenv("DB_SQLSERVER_USER")
DB_SQLSERVER_PASSWORD = os.getenv("DB_SQLSERVER_PASSWORD")
DB_SQLSERVER_DATABASE = os.getenv("DB_SQLSERVER_DATABASE")
DB_SQLSERVER_DRIVER = os.getenv("DB_SQLSERVER_DRIVER")

# ============================
# 2. 创建数据库连接
# ============================
print("正在连接 SQL Server 数据库...")
driver = DB_SQLSERVER_DRIVER.replace(" ", "+")
conn_str = (
    f"mssql+pyodbc://{DB_SQLSERVER_USER}:{DB_SQLSERVER_PASSWORD}"
    f"@{DB_SQLSERVER_HOST}:{DB_SQLSERVER_PORT}/{DB_SQLSERVER_DATABASE}"
    f"?driver={driver}&TrustServerCertificate=yes"
)
engine = create_engine(conn_str)

# ============================
# 3. 读取商品数据
# ============================
print("正在提取商品数据...")
query = text("""
    SELECT DISTINCT product_name
    FROM dbo.oms_order_dtl
    WHERE product_name IS NOT NULL
""")
df = pd.read_sql(query, engine)
print(f"共提取到 {len(df)} 个商品。")

# ============================
# 4. 读取敏感词列表
# ============================
sensitive_file = "suspected-sensitive-words.txt"
if os.path.exists(sensitive_file):
    with open(sensitive_file, "r", encoding="utf-8") as f:
        sensitive_words = [w.strip() for w in f if w.strip()]
    print(f"已加载 {len(sensitive_words)} 个敏感词。")
else:
    sensitive_words = []
    print("⚠ 未找到 suspected-sensitive-words.txt 文件，将不插入敏感词。")

# ============================
# 5. Qwen 评论生成函数
# ============================
def generate_comment_qwen(prompt, max_retries=5):
    for attempt in range(max_retries):
        try:
            response = requests.post(
                f"{QWEN_API_BASE}/chat/completions",
                headers={
                    "Authorization": f"Bearer {QWEN_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": QWEN_MODEL,
                    "messages": [
                        {"role": "system", "content": "你是一个真实的电商买家，写评价。"},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.9,
                    "max_tokens": 200
                },
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                content = data["choices"][0]["message"]["content"].strip()
                return content

            elif response.status_code == 503:
                wait_time = 2 ** attempt + random.uniform(0, 2)
                print(f"系统繁忙，等待 {wait_time:.1f} 秒后重试...")
                time.sleep(wait_time)

            else:
                print(f"API 请求失败 ({response.status_code}): {response.text}")
                break

        except Exception as e:
            print(f"请求异常：{e}")
            time.sleep(2)

    return "评论生成失败"

# ============================
# 6. 插入敏感词并高亮
# ============================
def insert_sensitive_words(comment: str, prob: float = 0.3) -> str:
    """
    随机插入敏感词到评论中，并用红色高亮显示
    """
    if not sensitive_words or not comment or comment == "评论生成失败":
        return comment

    if random.random() > prob:
        return comment  # 不插入敏感词

    # 随机插入 1~2 个敏感词
    insert_count = random.choice([1, 2])
    for _ in range(insert_count):
        word = random.choice(sensitive_words)
        insert_pos = random.randint(0, len(comment))
        # 高亮插入
        comment = comment[:insert_pos] + f"\033[91m{word}\033[0m" + comment[insert_pos:]
    return comment

# ============================
# 7. 批量生成评论
# ============================
batch_size = 50
total = len(df)

print("\n==============================")
print("开始批量生成商品评论")
print("==============================")

for start in range(0, total, batch_size):
    batch = df.iloc[start:start + batch_size]
    print(f"\n处理第 {start + 1} - {min(start + batch_size, total)} 条商品...")

    for i, row in batch.iterrows():
        product_name = row["product_name"]

        # 控制评论类型比例：70%好评, 30%差评
        tone = "好评" if random.random() < 0.7 else "差评"

        prefix = {
            "好评": "请生成一条积极、自然、带细节的好评：",
            "差评": "请生成一条语气不满但真实的差评："
        }[tone]

        raw_comment = generate_comment_qwen(f"{prefix}商品“{product_name}”。")

        # 随机概率插入敏感词（默认 30%）
        final_comment = insert_sensitive_words(raw_comment, prob=0.3)

        print(f"\n[{tone}] 商品：{product_name}\n评论：{final_comment}\n")

        # 防止接口限流
        time.sleep(random.uniform(1.5, 3.5))

print("\n✅ 全部评论生成完毕！")
engine.dispose()
print("\n数据库连接已关闭。")
