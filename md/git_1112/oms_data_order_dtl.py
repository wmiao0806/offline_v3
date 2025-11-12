import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv

def read_sqlserver_from_env():
    # 加载.env文件
    load_dotenv()
    
    # 从环境变量读取SQL Server配置
    db_host = os.getenv('DB_SQLSERVER_HOST')
    db_port = os.getenv('DB_SQLSERVER_PORT')
    db_user = os.getenv('DB_SQLSERVER_USER')
    db_password = os.getenv('DB_SQLSERVER_PASSWORD')
    db_database = os.getenv('DB_SQLSERVER_DATABASE')
    db_driver = os.getenv('DB_SQLSERVER_DRIVER')
    
    # 打印配置信息（隐藏密码）
    print("数据库配置:")
    print(f"  主机: {db_host}")
    print(f"  端口: {db_port}")
    print(f"  用户: {db_user}")
    print(f"  数据库: {db_database}")
    print(f"  驱动: {db_driver}")
    print("-" * 60)
    
    # 构建连接字符串
    connection_string = f"""
        DRIVER={{{db_driver}}};
        SERVER={db_host},{db_port};
        DATABASE={db_database};
        UID={db_user};
        PWD={db_password};
        TrustServerCertificate=yes;
        Encrypt=yes;
    """
    
    try:
        # 建立连接
        print("正在连接SQL Server数据库...")
        conn = pyodbc.connect(connection_string)
        
        # 使用pandas读取数据
        print("正在读取dbo.oms_order_dtl表数据...")
        df = pd.read_sql("SELECT * FROM dbo.oms_order_dtl", conn)
        
        print("=" * 80)
        print("dbo.oms_order_dtl 表数据")
        print("=" * 80)
        
        # 显示数据基本信息
        print(f"数据形状: {df.shape} (行数: {df.shape[0]}, 列数: {df.shape[1]})")
        print(f"\n列名: {list(df.columns)}")
        print("\n前10条记录:")
        print("-" * 80)
        
        # 设置pandas显示选项
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 30)
        pd.set_option('display.max_rows', 10)
        
        # 打印数据
        print(df.head(10))
        
        # 显示数据类型信息
        print("\n" + "=" * 80)
        print("数据类型信息:")
        print(df.dtypes)
        
    except pyodbc.Error as e:
        print(f"数据库连接错误: {e}")
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("\n数据库连接已关闭")

if __name__ == "__main__":
    read_sqlserver_from_env()