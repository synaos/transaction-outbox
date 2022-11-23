package com.synaos.transactionoutbox.acceptance;

import com.synaos.transactionoutbox.Dialect;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

@SuppressWarnings("WeakerAccess")
@Testcontainers
@Disabled //todo fixme
class TestMySql5 extends AbstractAcceptanceTest {

  @Container
  @SuppressWarnings("rawtypes")
  private static final JdbcDatabaseContainer container =
          new MySQLContainer<>("mysql:5").withStartupTimeout(Duration.ofHours(1));

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
            .dialect(Dialect.MY_SQL_5)
            .driverClassName("com.mysql.cj.jdbc.Driver")
            .url(container.getJdbcUrl())
            .user(container.getUsername())
            .password(container.getPassword())
            .build();
  }
}