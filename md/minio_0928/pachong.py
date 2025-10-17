
"""
邓州 7 天天气 · 每 10 秒增量更新（兼容 PG<9.5）
"""
import time, signal, sys
import requests, psycopg2, re
from lxml import etree

# ---------- 数据库 ----------
DB_CFG = dict(
    dbname='postgres',
    user='postgres',
    password='root',
    host='192.168.200.30',
    port=5432
)

# ---------- SQL ----------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.weather7days (
    date        DATE PRIMARY KEY,
    weekday     VARCHAR(3),
    weather     VARCHAR(10),
    wind_dir    VARCHAR(10),
    wind_lvl    VARCHAR(10),
    temp_high   SMALLINT,
    temp_low    SMALLINT
);
"""

# 低版本兼容：先更新，没命中再插入
UPSERT_SQL = """
UPDATE public.weather7days
SET weekday     = %(weekday)s,
    weather     = %(weather)s,
    wind_dir    = %(wind_dir)s,
    wind_lvl    = %(wind_lvl)s,
    temp_high   = %(temp_high)s,
    temp_low    = %(temp_low)s
WHERE date = %(date)s;
INSERT INTO public.weather7days
(date, weekday, weather, wind_dir, wind_lvl, temp_high, temp_low)
SELECT %(date)s, %(weekday)s, %(weather)s, %(wind_dir)s, %(wind_lvl)s,
       %(temp_high)s, %(temp_low)s
WHERE NOT EXISTS (SELECT 1 FROM public.weather7days WHERE date = %(date)s);
"""

# ---------- 抓取 ----------
def crawl():
    url = 'https://weather.cma.cn/web/weather/57178.html'
    headers = {'User-Agent': 'Mozilla/5.0'}
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    resp.encoding = 'utf-8'

    txt = ''.join(etree.HTML(resp.text)
                  .xpath('/html/body/div[1]/div[2]/div[1]/div[1]/div[2]')[0]
                  .itertext())

    pat = re.compile(r'星期([\u4e00-\u9fa5])\s*(\d{2}/\d{2})\s*'
                     r'([\u4e00-\u9fa5]+)\s*'
                     r'([\u4e00-\u9fa5]{2}风)\s*'
                     r'([\u4e00-\u9fa5\d~级]+)\s*'
                     r'(\d{1,2})℃\s*(\d{1,2})℃', re.S)

    rows = []
    for w, d, we, wd, wl, h, l in pat.findall(txt):
        mon, day = d.split('/')
        rows.append(dict(
            date=f'2025-{int(mon):02d}-{int(day):02d}',
            weekday=w,
            weather=we,
            wind_dir=wd,
            wind_lvl=wl,
            temp_high=int(h),
            temp_low=int(l)
        ))
    return rows

# ---------- 主循环 ----------
def main():
    with psycopg2.connect(**DB_CFG) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)

    while True:
        try:
            t0 = time.time()
            data = crawl()
            with psycopg2.connect(**DB_CFG) as conn:
                with conn.cursor() as cur:
                    for row in data:
                        cur.execute(UPSERT_SQL, row)
                    print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} '
                          f'已更新 {len(data)} 条记录')
            time.sleep(max(0, 10 - (time.time() - t0)))
        except KeyboardInterrupt:
            print('用户中断，退出')
            sys.exit(0)
        except Exception as e:
            print('抓取异常：', e)
            time.sleep(10)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, lambda *args: sys.exit(0))
    main()