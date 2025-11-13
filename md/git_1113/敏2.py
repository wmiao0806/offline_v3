import pyodbc
import pandas as pd
import os
import random
import time
import requests
import re
from dotenv import load_dotenv

def generate_all_product_reviews_to_csv(batch_size=50, sensitive_prob=0.3, output_file="generated_reviews.csv"):
    # 加载 .env 文件
    load_dotenv()

    # SQL Server 配置
    db_host = os.getenv('DB_SQLSERVER_HOST')
    db_port = os.getenv('DB_SQLSERVER_PORT')
    db_user = os.getenv('DB_SQLSERVER_USER')
    db_password = os.getenv('DB_SQLSERVER_PASSWORD')
    db_database = os.getenv('DB_SQLSERVER_DATABASE')
    db_driver = os.getenv('DB_SQLSERVER_DRIVER')

    # Qwen API 配置
    qwen_api_key = os.getenv('QWEN_API_KEY')
    qwen_api_base = os.getenv('QWEN_API_BASE')
    qwen_model = os.getenv('QWEN_MODEL')

    # 敏感词文件
    sensitive_file = "suspected-sensitive-words.txt"
    if os.path.exists(sensitive_file):
        with open(sensitive_file, "r", encoding="utf-8") as f:
            sensitive_words = [w.strip() for w in f if w.strip()]
        print(f"已加载 {len(sensitive_words)} 个敏感词")
    else:
        sensitive_words = []
        print("⚠ 未找到 suspected-sensitive-words.txt 文件")

    # 数据库连接字符串
    connection_string = f"""
        DRIVER={{{db_driver}}};
        SERVER={db_host},{db_port};
        DATABASE={db_database};
        UID={db_user};
        PWD={db_password};
        TrustServerCertificate=yes;
        Encrypt=yes;
    """

    def split_chinese_part(text):
        """拆分英文/中文"""
        chinese_pattern = re.compile(r'[\u4e00-\u9fff]')
        match = chinese_pattern.search(text)
        if match:
            chinese_start = match.start()
            english_part = text[:chinese_start].strip()
            chinese_part = text[chinese_start:].strip()
            return english_part, chinese_part
        else:
            return text.strip(), ""

    def call_qwen_api_with_retry(product_info, review_type, max_retries=5, max_wait_time=10):
        """调用 Qwen API 带重试"""
        prompt = f"请为以下商品生成一条真实、自然的{'正面' if review_type=='positive' else '负面'}评价，评价内容具体，包含使用体验和优缺点：{product_info}"
        headers = {"Authorization": f"Bearer {qwen_api_key}", "Content-Type": "application/json"}
        data = {"model": qwen_model, "messages":[{"role":"user","content":prompt}],
                "max_tokens":150, "temperature":0.8}
        for attempt in range(max_retries):
            try:
                response = requests.post(f"{qwen_api_base}/chat/completions", headers=headers, json=data, timeout=30)
                if response.status_code == 200:
                    return response.json()['choices'][0]['message']['content'].strip()
                elif response.status_code == 503:
                    wait_time = min((2**attempt) + random.random(), max_wait_time)
                    print(f"API繁忙，等待 {wait_time:.2f}s 后重试...")
                    time.sleep(wait_time)
                else:
                    print(f"API失败 {response.status_code}: {response.text}")
                    return None
            except Exception as e:
                print(f"API异常: {e}")
                time.sleep(min((2**attempt)+random.random(), max_wait_time))
        return None

    def insert_sensitive_words(comment, prob=sensitive_prob):
        if not sensitive_words or not comment:
            return comment
        if random.random() > prob:
            return comment
        for _ in range(random.choice([1,2])):
            word = random.choice(sensitive_words)
            pos = random.randint(0, len(comment))
            comment = comment[:pos] + f"\033[91m{word}\033[0m" + comment[pos:]
        return comment

    try:
        print("正在连接 SQL Server 数据库...")
        conn = pyodbc.connect(connection_string)
        df = pd.read_sql("SELECT product_name FROM dbo.oms_order_dtl WHERE product_name IS NOT NULL", conn)

        if df.empty:
            print("表中没有数据")
            return

        print(f"共 {len(df)} 条商品，将分批处理，每批 {batch_size} 条")

        results = []

        # 分批处理
        for start in range(0, len(df), batch_size):
            batch = df.iloc[start:start+batch_size]
            print(f"\n处理第 {start+1} - {start+len(batch)} 条商品...")

            for idx, row in batch.iterrows():
                product_name = str(row['product_name']).strip()
                parts = product_name.split('丨')
                brand = parts[0].strip() if len(parts) > 0 else ""
                product_info = parts[1].strip() if len(parts) > 1 else product_name
                category, item_desc = split_chinese_part(product_info)

                review_type = random.choice(["positive","negative"])
                review = call_qwen_api_with_retry(f"{brand} {category} {item_desc}", review_type)
                if review:
                    review = insert_sensitive_words(review)
                    results.append({
                        "tone": "好评" if review_type=="positive" else "差评",
                        "brand": brand,
                        "category": category,
                        "product_name": item_desc,
                        "review": review
                    })
                    print(f"[{'好评' if review_type=='positive' else '差评'}] 品牌：{brand}，品类名：{category}，商品名：{item_desc}，评论：{review}")
                else:
                    print("未生成评论")

                time.sleep(2)  # 控制请求频率

        # 保存 CSV
        if results:
            out_df = pd.DataFrame(results)
            out_df.to_csv(output_file, index=False, encoding="utf-8-sig")
            print(f"\n✅ 评论结果已保存到 {output_file}")

    except pyodbc.Error as e:
        print(f"数据库连接错误: {e}")
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    generate_all_product_reviews_to_csv()
