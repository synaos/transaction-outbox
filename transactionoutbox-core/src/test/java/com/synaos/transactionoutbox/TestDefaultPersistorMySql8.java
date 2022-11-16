package com.synaos.transactionoutbox;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

@Testcontainers
class TestDefaultPersistorMySql8 extends AbstractDefaultPersistorTest {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
          new MySQLContainer<>("mysql:8").withStartupTimeout(Duration.ofHours(1));

  private DefaultPersistor persistor = DefaultPersistor.builder().dialect(Dialect.MY_SQL_8).build();
  private TransactionManager txManager =
          TransactionManager.fromConnectionDetails(
                  "com.mysql.cj.jdbc.Driver",
                  container.getJdbcUrl(),
                  container.getUsername(),
                  container.getPassword());

  @Override
  protected DefaultPersistor persistor() {
    return persistor;
  }

  @Override
  protected TransactionManager txManager() {
    return txManager;
  }

  @Override
  protected Dialect dialect() {
    return Dialect.MY_SQL_8;
  }
}
