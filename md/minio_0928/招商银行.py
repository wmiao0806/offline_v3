# import requests
# import json
# import logging
# import threading
# import psycopg2
# from psycopg2 import sql
# from psycopg2.extras import execute_batch
#
# # 配置日志
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler('spider_amap_weather.log'),
#         logging.StreamHandler()
#     ]
# )
# logger = logging.getLogger(__name__)
#
# db_params = {
#     "host": "192.168.200.30",
#     "port": "5432",
#     "database": "spider_db",
#     "user": "postgres",
#     "password": "root"
# }
#
# # API URL
# CMB_EXCHANGE_RATE_URL = 'https://fx.cmbchina.com/api/v1/fx/rate'
#
# headers = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#     'Accept': 'application/json, text/plain, */*',
#     'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
# }
#
#
# def fetch_data_2postgresql():
#     resp = requests.get(CMB_EXCHANGE_RATE_URL)
#     resp_json = resp.json()
#     res = []
#     for i in resp_json['body']:
#         ccy_zh = i['ccyNbr']
#         ccy_en = i['ccyNbrEng'].split(' ')[1]
#         date_part = i['ratDat'].replace('年', '-').replace('月', '-').replace('日', '')
#         time_part = i['ratTim']
#         ct_time = f"{date_part} {time_part}"
#         data = {
#             "ccy_zh": i['ccyNbr'],
#             "ccy_en": i['ccyNbrEng'].split(' ')[1],
#             "rtbBid": i['rtbBid'],
#             "rthOfr": i['rthOfr'],
#             "rtcOfr": i['rtcOfr'],
#             "rthBid": i['rthBid'],
#             "rtcBid": i['rtcBid'],
#             "ratTim": i['ratTim'],
#             "ratDat": i['ratDat']
#         }
#         res.append((ccy_zh, ccy_en, ct_time, json.dumps(data)))
#     if not res:
#         logger.info("No valid data to insert")
#         return 0
#     with psycopg2.connect(**db_params) as conn:
#         with conn.cursor() as cursor:
#             # SQL插入语句
#             insert_query = sql.SQL("""
#                     INSERT INTO public.spider_exchange_rate_dtl (ccy_zh, ccy_en,ct_time, data)
#                     VALUES (%s, %s, %s, %s)
#                 """)
#
#             # 批量插入数据
#             execute_batch(cursor, insert_query, res)
#             conn.commit()
#
#     logger.info(f"成功插入 {len(res)} 条记录")
#
#
# def main():
#     fetch_data_2postgresql()
#     threading.Timer(10, main).start()
#
#
# if __name__ == '__main__':
#     main()



#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
招商银行外汇牌价 · 每 10 秒增量更新（低版本 PostgreSQL 兼容）
先删旧表再重建，保证结构最新
"""
import time
import requests
import json
import logging
import threading
import psycopg2

# ---------- 日志 ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('spider_amap_weather.log'),
              logging.StreamHandler()])
logger = logging.getLogger(__name__)

# ---------- 数据库 ----------
DB_CFG = dict(host='192.168.200.30', port=5432, database='spider_db',
              user='postgres', password='root')

# ---------- 建库+删表+建表 ----------
def init_db():
    # 1. 建库（非事务）
    init = {**DB_CFG, 'database': 'postgres'}
    conn = psycopg2.connect(**init)
    conn.set_session(autocommit=True)
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname='spider_db';")
        if not cur.fetchone():
            cur.execute("CREATE DATABASE spider_db;")
            logger.info('已自动创建数据库 spider_db')
    conn.close()

    # 2. 删旧表再建新表
    with psycopg2.connect(**DB_CFG) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS public.spider_exchange_rate_dtl;")
            cur.execute("""
                CREATE TABLE public.spider_exchange_rate_dtl (
                    id      BIGSERIAL PRIMARY KEY,
                    ccy_zh  VARCHAR(20) NOT NULL,
                    ccy_en  VARCHAR(3)  NOT NULL,
                    ct_time TIMESTAMP   NOT NULL,
                    data    TEXT        NOT NULL
                );
            """)
            cur.execute("""
                CREATE UNIQUE INDEX uk_ccy_time
                ON public.spider_exchange_rate_dtl (ccy_en, ct_time);
            """)
            logger.info('已删除旧表并重新创建')

# ---------- 抓取+低版本兼容写入 ----------
def fetch():
    url = 'https://fx.cmbchina.com/api/v1/fx/rate'
    try:
        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        resp.raise_for_status()
        body = resp.json()['body']
    except Exception as e:
        logger.error('抓取失败: %s', e)
        return 0

    res = []
    for i in body:
        date_part = i['ratDat'].replace('年', '-').replace('月', '-').replace('日', '')
        ct_time = f"{date_part} {i['ratTim']}"
        res.append((
            i['ccyNbr'],
            i['ccyNbrEng'].split()[-1],
            ct_time,
            json.dumps(i, ensure_ascii=False)
        ))

    if not res:
        logger.info('无有效数据')
        return 0

    # 低版本兼容：UPDATE → INSERT
    with psycopg2.connect(**DB_CFG) as conn:
        with conn.cursor() as cur:
            for row in res:
                cur.execute("""
                    UPDATE public.spider_exchange_rate_dtl
                    SET ccy_zh = %s, data = %s
                    WHERE ccy_en = %s AND ct_time = %s;
                """, (row[0], row[3], row[1], row[2]))

                cur.execute("""
                    INSERT INTO public.spider_exchange_rate_dtl (ccy_zh, ccy_en, ct_time, data)
                    SELECT %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM public.spider_exchange_rate_dtl
                        WHERE ccy_en = %s AND ct_time = %s
                    );
                """, row + (row[1], row[2]))     # 后两个给 NOT EXISTS 用

            logger.info('成功处理 %s 条记录', len(res))
    return len(res)

# ---------- 10 秒循环 ----------
def main():
    init_db()
    while True:
        t0 = time.time()
        fetch()
        time.sleep(max(0, 10 - (time.time() - t0)))

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info('用户中断，退出')