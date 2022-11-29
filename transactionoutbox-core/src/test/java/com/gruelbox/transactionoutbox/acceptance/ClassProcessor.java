package com.gruelbox.transactionoutbox.acceptance;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClassProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClassProcessor.class);

  static final List<String> PROCESSED = new CopyOnWriteArrayList<>();

  void process(String itemId) {
    LOGGER.info("Processing work: {}", itemId);
    PROCESSED.add(itemId);
  }
}
