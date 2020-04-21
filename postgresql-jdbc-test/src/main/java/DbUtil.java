import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DbUtil {
    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
//        return DriverManager.getConnection("jdbc:postgresql://169.254.102.187:5435/pg1", "sa", "123456");
        return DriverManager.getConnection("jdbc:postgresql://10.201.8.8:5435/pg1", "sa", "123456");
    }
}
