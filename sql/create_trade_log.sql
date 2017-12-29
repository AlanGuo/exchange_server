CREATE TABLE `slice_balance_example` (
    `id`            INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `user_id`       INT UNSIGNED NOT NULL,
    `asset`         VARCHAR(30) NOT NULL,
    `t`             TINYINT UNSIGNED NOT NULL,
    `balance`       DECIMAL(40,20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `slice_order_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `t`             TINYINT UNSIGNED NOT NULL,
    `side`          TINYINT UNSIGNED NOT NULL,
    `create_time`   DOUBLE NOT NULL,
    `update_time`   DOUBLE NOT NULL,
    `user_id`       INT UNSIGNED NOT NULL,
    `market`        VARCHAR(30) NOT NULL,
    `source`        VARCHAR(30) NOT NULL,
    `price`         DECIMAL(40,8) NOT NULL,
    `amount`        DECIMAL(40,8) NOT NULL,
    `taker_fee`     DECIMAL(40,4) NOT NULL,
    `maker_fee`     DECIMAL(40,4) NOT NULL,
    `left`          DECIMAL(40,8) NOT NULL,
    `frozen`        DECIMAL(40,8) NOT NULL,
    `deal_stock`    DECIMAL(40,8) NOT NULL,
    `deal_money`    DECIMAL(40,16) NOT NULL,
    `deal_fee`      DECIMAL(40,20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `slice_history` (
    `id`            INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `time`          BIGINT NOT NULL,
    `end_oper_id`   BIGINT UNSIGNED NOT NULL,
    `end_order_id`  BIGINT UNSIGNED NOT NULL,
    `end_deals_id`  BIGINT UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `operlog_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `time`          DOUBLE NOT NULL,
    `detail`        TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
