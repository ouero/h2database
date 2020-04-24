import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertBlackHoleTest {
    public static void main(String[] args) throws SQLException {
        Connection c = null;
        PreparedStatement stmt = null;
        try {
            c = DbUtil.getConnection();
            c.setAutoCommit(false);
            String sql = "insert into  test_black_hole values (?,?)";
            stmt = c.prepareStatement(sql);
            int start = 1;
            int end = 10_0000;
            int i = start;
            long l=System.currentTimeMillis();
            for (; i <= end; i++) {
                stmt.setInt(1, i);
                stmt.setString(2, "jack" + i);
                stmt.addBatch();
                if (i != 0 && i % 10000 == 0) {
                    stmt.executeBatch();
                }
            }
            if (i % 10 != 0) {
                stmt.executeBatch();
            }
            c.commit();
            System.out.println(System.currentTimeMillis()-l);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            stmt.close();
            c.close();
        }
    }
}
