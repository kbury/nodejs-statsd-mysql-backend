-- Stadard DELIMITER is $$

-- Counters statistics table
CREATE TABLE counters_statistics (
    id int NOT NULL IDENTITY(1,1),
    timestamp bigint NOT NULL,
    name varchar(255) NOT NULL,
    value decimal(18,3) NOT NULL,
    username varchar(100) COLLATE Latin1_General_CI_AS NULL,
    ip_address varchar(20) COLLATE Latin1_General_CI_AS NULL,
    user_agent varchar(260) COLLATE Latin1_General_CI_AS NULL
PRIMARY KEY (id) )$$
