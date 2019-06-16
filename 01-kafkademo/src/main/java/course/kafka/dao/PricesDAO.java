package course.kafka.dao;

import course.kafka.model.StockPrice;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j

public class PricesDAO {
    public static final String DB_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public static final String DB_URL = "jdbc:sqlserver://localhost;databaseName=kafka_demo;user=sa;password=SApass123;";
    public static final String DB_USER = "sa";
    public static final String DB_PASSWORD = "SApass123";


    List<StockPrice> prices = new CopyOnWriteArrayList<>();

    public void reload() {
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException ex) {
            log.error("MS SQL Server DB Driver not found", ex);
        }

        try(Connection con = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            Statement statement = con.createStatement();
        ) {
            ResultSet rs = statement.executeQuery("SELECT * FROM Prices");
            while (rs.next()) {
                prices.add(new StockPrice(
                        rs.getInt("id"),
                        rs.getString("symbol"),
                        rs.getString("name"),
                        rs.getDouble("price"),
                        rs.getTimestamp("timestamp")
                ));
            }

        } catch (SQLException e) {
            log.error("Connection to MS SQL Server URL: {} cannot be established.\n{}", DB_URL, e);
        }
    }

    public void printData() {
        prices.forEach(price -> {
                    System.out.printf("| %10d | %5.5s | %20.20s | %10.2f | %td.%<tm.%<ty-%<TH:%<TM:%<TS |",
                            price.getId(),
                            price.getSymbol(),
                            price.getName(),
                            price.getPrice(),
                            price.getTimestamp());
                }
        );

        }


    public static void main(String[] args) {
        PricesDAO dao = new PricesDAO();
        dao.reload();
        dao.printData();
    }
}


