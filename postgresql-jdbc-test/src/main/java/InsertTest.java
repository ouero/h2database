import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertTest {
    public static void main(String[] args) throws SQLException {
        Connection c = null;
        PreparedStatement stmt = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager.getConnection("jdbc:postgresql://2.0.1.55:5435/h2_db2", "sa", "123456");
            String sql = "insert into  test values (?,?)";
            stmt = c.prepareStatement(sql);
            int i = 1;
            for (; i <= 1000; i++) {
                stmt.setInt(1, i);
                stmt.setString(2, "jack" + i);
                stmt.addBatch();
                if (i != 0 && i % 10 == 0) {
                    stmt.executeBatch();
                }
            }
            if (i % 10 != 0) {
                stmt.executeBatch();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            stmt.close();
            c.close();
        }
    }
}
