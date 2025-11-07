package work1107;

import java.sql.Connection;
import java.sql.DriverManager;

public class MySQLTest {
    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection conn = DriverManager.getConnection(
                "jdbc:mysql://192.168.200.32:3306/work?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf8",
                "root",
                "root"
        )) {
            System.out.println("✅ 连接成功: " + conn.getMetaData().getDatabaseProductName());
        }
    }
}
