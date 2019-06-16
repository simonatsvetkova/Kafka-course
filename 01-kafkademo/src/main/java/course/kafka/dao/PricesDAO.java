package course.kafka.dao;

import course.kafka.model.StockPrice;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j

public class PricesDAO {
    public static final String DB_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public static final String DB_URL = "jdbc:sqlserver://localhost;databaseName=kafka_demo;user=sa;password=SApass123;";
    public static final String DB_USER = "sa";
    public static final String DB_PASSWORD = "SApass123";

    public static final String SELECT_ALL_PRICES_SQL = "SELECT * FROM Prices";
    public static final String INSERT_INTO_PRICES_SQL = "INSERT INTO Prices (symbol, name, price) VALUES (?, ?, ?)";


    private Connection con;
    private PreparedStatement selectAllStatement;
    private PreparedStatement insertIntoStatement;


    List<StockPrice> prices = new CopyOnWriteArrayList<>();

/*
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

 */


    public void init() throws SQLException {
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException ex) {
            log.error("MS SQL Server db driver not found", ex);
        }

        try {
            con = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            selectAllStatement = con.prepareStatement(SELECT_ALL_PRICES_SQL);
            insertIntoStatement = con.prepareStatement(INSERT_INTO_PRICES_SQL);
        } catch (SQLException e) {
            log.error("Connection to MS SQL Server URL: {} cannot be established.\n{}", DB_URL, e);
            throw e;
        }
    }

    public void close() {
        try {
            if (!insertIntoStatement.isClosed()) {
                insertIntoStatement.close();
            }
            if (!selectAllStatement.isClosed()) {
                selectAllStatement.close();
            }
            if (!con.isClosed()) {
                con.close();
            }
        } catch (SQLException e) {
            log.error("Error closing connection to SQL Server URL:{}.\n{}", DB_URL, e);
        }
    }

    public void reload() throws SQLException {
        try {
            ResultSet rs = selectAllStatement.executeQuery();
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
            log.error("Error executing SQL statement.", e);
            throw e;
        }
    }


    public int insertPrice(StockPrice price) throws SQLException {
        insertIntoStatement.setString(1, price.getSymbol());
        insertIntoStatement.setString(2, price.getName());
        insertIntoStatement.setDouble(3, price.getPrice());
        return insertIntoStatement.executeUpdate();
    }

    public void printData() {
        prices.forEach(price -> {
                    System.out.printf("| %10d | %5.5s | %20.20s | %10.2f | %td.%<tm.%<ty-%<TH:%<TM:%<TS | \n",
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
        List<StockPrice> stocks = Arrays.asList(
                new StockPrice("VMW", "VMWare", 215.35),
                new StockPrice("GOOG", "Google", 309.17),
                new StockPrice("CTXS", "Citrix Systems, Inc.", 112.11),
                new StockPrice("DELL", "Dell Inc.", 92.93),
                new StockPrice("MSFT", "Microsoft", 255.19),
                new StockPrice("ORCL", "Oracle", 115.72),
                new StockPrice("RHT", "Red Hat", 111.27)
        );
        try {
            dao.init();
            for(StockPrice sp : stocks) {
                dao.insertPrice(sp);
            }
            dao.reload();
            dao.printData();
        } catch (SQLException e){
            log.error("DB Error:", e);
        } finally {
            dao.close();
        }
    }
}


