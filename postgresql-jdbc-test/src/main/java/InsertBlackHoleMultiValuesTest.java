import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class InsertBlackHoleMultiValuesTest {
    public static void main(String[] args) throws SQLException {
        Connection c = null;
        PreparedStatement stmt = null;
        try {
            c = DbUtil.getConnection();
            c.setAutoCommit(false);
//            String sql = FileUtils.readFileSToString(new File("D:\\gold\\h2database\\postgresql-jdbc-test\\src\\main\\resources\\insertSql"), "utf-8");
            String sql = "insert into test_black_hole values (1,'jack1'),(2,'jack2')";
            Statement statement = c.createStatement();
            long l = System.currentTimeMillis();
            statement.execute(sql);
            c.commit();

            statement = c.createStatement();
            statement.execute(sql);
            c.commit();
            System.out.println(System.currentTimeMillis() - l);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (c != null) {
                c.close();
            }
        }
    }
}
