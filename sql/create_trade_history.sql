CREATE TABLE `balance_history_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `time`          DOUBLE NOT NULL,
    `user_id`       INT UNSIGNED NOT NULL,
    `asset`         VARCHAR(40) NOT NULL,
    `business`      VARCHAR(40) NOT NULL,
    `change`        DECIMAL(40,8) NOT NULL,
    `balance`       DECIMAL(40,20) NOT NULL,
    `detail`        TEXT NOT NULL,
    INDEX `idx_user_time` (`user_id`, `time`),
    INDEX `idx_user_business_time` (`user_id`, `business`, `time`),
    INDEX `idx_user_asset_business_time` (`user_id`, `asset`, `business`, `time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- split by user_id
CREATE TABLE `order_history_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `create_time`   DOUBLE NOT NULL,
    `finish_time`   DOUBLE NOT NULL,
    `user_id`       INT UNSIGNED NOT NULL,
    `market`        VARCHAR(40) NOT NULL,
    `source`        VARCHAR(40) NOT NULL,
    `t`             TINYINT UNSIGNED NOT NULL,
    `side`          TINYINT UNSIGNED NOT NULL,
    `price`         DECIMAL(40,8) NOT NULL,
    `amount`        DECIMAL(40,8) NOT NULL,
    `taker_fee`     DECIMAL(40,4) NOT NULL,
    `maker_fee`     DECIMAL(40,4) NOT NULL,
    `deal_stock`    DECIMAL(40,8) NOT NULL,
    `deal_money`    DECIMAL(40,16) NOT NULL,
    `deal_fee`      DECIMAL(40,20) NOT NULL,
    INDEX `idx_user_market_time` (`user_id`, `market`, `create_time`),
    INDEX `idx_user_market_side_time` (`user_id`, `market`, `side`, `create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- split by id, aka orer_id
CREATE TABLE `order_detail_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `create_time`   DOUBLE NOT NULL,
    `finish_time`   DOUBLE NOT NULL,
    `user_id`       INT UNSIGNED NOT NULL,
    `market`        VARCHAR(40) NOT NULL,
    `source`        VARCHAR(40) NOT NULL,
    `t`             TINYINT UNSIGNED NOT NULL,
    `side`          TINYINT UNSIGNED NOT NULL,
    `price`         DECIMAL(40,8) NOT NULL,
    `amount`        DECIMAL(40,8) NOT NULL,
    `taker_fee`     DECIMAL(40,4) NOT NULL,
    `maker_fee`     DECIMAL(40,4) NOT NULL,
    `deal_stock`    DECIMAL(40,8) NOT NULL,
    `deal_money`    DECIMAL(40,16) NOT NULL,
    `deal_fee`      DECIMAL(40,20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- split by order_id
CREATE TABLE `order_deal_history_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `time`          DOUBLE NOT NULL,
    `user_id`       INT UNSIGNED NOT NULL,
    `deal_id`       BIGINT UNSIGNED NOT NULL,
    `order_id`      BIGINT UNSIGNED NOT NULL,
    `deal_order_id` BIGINT UNSIGNED NOT NULL,
    `role`          TINYINT UNSIGNED NOT NULL,
    `price`         DECIMAL(40,8) NOT NULL,
    `amount`        DECIMAL(40,8) NOT NULL,
    `deal`          DECIMAL(40,16) NOT NULL,
    `fee`           DECIMAL(40,20) NOT NULL,
    `deal_fee`      DECIMAL(40,20) NOT NULL,
    INDEX `idx_order_id` (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- split by user_id
CREATE TABLE `user_deal_history_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `time`          DOUBLE NOT NULL,
    `user_id`       INT UNSIGNED NOT NULL,
    `market`        VARCHAR(40) NOT NULL,
    `deal_id`       BIGINT UNSIGNED NOT NULL,
    `order_id`      BIGINT UNSIGNED NOT NULL,
    `deal_order_id` BIGINT UNSIGNED NOT NULL,
    `side`          TINYINT UNSIGNED NOT NULL,
    `role`          TINYINT UNSIGNED NOT NULL,
    `price`         DECIMAL(40,8) NOT NULL,
    `amount`        DECIMAL(40,8) NOT NULL,
    `deal`          DECIMAL(40,16) NOT NULL,
    `fee`           DECIMAL(40,20) NOT NULL,
    `deal_fee`      DECIMAL(40,20) NOT NULL,
    INDEX `idx_user_market_time` (`user_id`, `market`, `time`),
    INDEX `idx_user_market_side_time` (`user_id`, `market`, `side`, `time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
