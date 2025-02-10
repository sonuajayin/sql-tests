package com.example;

import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.ext.mssql.MsSqlDataTypeFactory;
import org.dbunit.operation.DatabaseOperation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AddNumbersTests {
    private static Connection connection;
    private static IDatabaseConnection dbUnitConnection;
    private static final Properties properties = new Properties();
    private static IDataSet dataSet;

    @BeforeAll
    static void setUp() throws Exception {
        loadDatabaseConfig();

        // Create the database connection
        connection = DriverManager.getConnection(
                properties.getProperty("db.url"),
                properties.getProperty("db.username"),
                properties.getProperty("db.password")
        );

        // Create DBUnit connection
        dbUnitConnection = new DatabaseConnection(connection);

        // Configure the database connection for SQL Server
        DatabaseConfig config = dbUnitConnection.getConfig();
        config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new MsSqlDataTypeFactory());

        // Load the dataset
        dataSet = new FlatXmlDataSetBuilder().build(
                AddNumbersTests.class.getResourceAsStream("/dataset.xml")
        );

        try {
            // Clean any existing data and insert new test data
            DatabaseOperation.CLEAN_INSERT.execute(dbUnitConnection, dataSet);
        } catch (Exception e) {
            throw new RuntimeException("Error setting up test data", e);
        }
    }

    @Test
    void testAddNumbersProcedure() throws Exception {
        try (CallableStatement stmt = connection.prepareCall("{ call AddNumbers(?, ?, ?) }")) {
            stmt.setInt(1, 5);
            stmt.setInt(2, 3);
            stmt.registerOutParameter(3, Types.INTEGER);
            stmt.execute();

            int result = stmt.getInt(3);
            assertEquals(8, result);
        }
    }

    @Test
    void testGetNumberById() throws Exception {
        // Get expected data
        ITable expectedTable = dataSet.getTable("Numbers");

        // Use SELECT FROM syntax for table-valued function
        try (PreparedStatement stmt = connection.prepareStatement(
                "SELECT * FROM dbo.GetNumberById(?)")) {

            // Test id=1
            stmt.setInt(1, 1);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                assertEquals(Integer.parseInt((String) expectedTable.getValue(0, "id")), rs.getInt("id"));
                assertEquals(Integer.parseInt((String) expectedTable.getValue(0, "value1")), rs.getInt("value1"));
                assertEquals(Integer.parseInt((String) expectedTable.getValue(0, "value2")), rs.getInt("value2"));
            }

            // Test id=2
            stmt.setInt(1, 2);
            rs = stmt.executeQuery();

            if (rs.next()) {
                assertEquals(Integer.parseInt((String) expectedTable.getValue(1, "id")), rs.getInt("id"));
                assertEquals(Integer.parseInt((String) expectedTable.getValue(1, "value1")), rs.getInt("value1"));
                assertEquals(Integer.parseInt((String) expectedTable.getValue(1, "value2")), rs.getInt("value2"));
            }
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (dbUnitConnection != null) {
            // Clean up test data
            IDataSet dataSet = new FlatXmlDataSetBuilder().build(
                    AddNumbersTests.class.getResourceAsStream("/dataset.xml")
            );
            DatabaseOperation.DELETE_ALL.execute(dbUnitConnection, dataSet);
            dbUnitConnection.close();
        }

        if (connection != null) {
            connection.close();
        }
    }

    static void loadDatabaseConfig() throws IOException {
        try (FileInputStream fis = new FileInputStream("src/test/resources/dbconfig.properties")) {
            properties.load(fis);
        }
    }
}
