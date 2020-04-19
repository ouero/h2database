import java.sql.*;

public class SelectTest {
    public static void main(String[] args) throws SQLException {
        Connection c = null;
        Statement stmt = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager.getConnection("jdbc:postgresql://2.0.1.55:5435/h2_db2", "sa", "123456");
            stmt = c.createStatement();
            String sql = "SELECT * from test";
            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                int anInt = resultSet.getInt(1);
                String string = resultSet.getString(2);
                System.out.printf(anInt + "," + string);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            stmt.close();
            c.close();
        }
    }

}
