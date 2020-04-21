import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SelectTest {
    public static void main(String[] args) throws SQLException {
        Connection c = null;
        PreparedStatement stmt = null;
        try {
            c = DbUtil.getConnection();
            c.setAutoCommit(false);
            String sql = "SELECT * from m_hz_czrkjbxx_stream ";
            stmt = c.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(2_0000);
            stmt.setMaxRows(2_0000);
            ResultSet resultSet = stmt.executeQuery();
            int columnCount = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                System.out.println(resultSet.getMetaData().getColumnName(i) + "," + resultSet.getMetaData().getColumnType(i) + "," + resultSet.getMetaData().getColumnTypeName(i));
            }
            int sum = 0;
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    Object object = resultSet.getObject(i);
                    System.out.printf(object == null ? "" : object.toString() + ",");
                }
                System.out.println();
                sum++;
            }
            System.out.println("total row:" + sum);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            stmt.close();
            c.close();
        }
    }

}
