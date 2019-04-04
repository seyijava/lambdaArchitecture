CREATE TABLE "HourlyTransactionRollUpPerMerchant" (
    id bigint,
    day int,
    hour int,
    merchant text,
    min int,
    month int,
    totalcount int,
    totalsum double,
    trxntimestamp timestamp,
    year int,
   PRIMARY KEY ((hour, merchant), id)
) WITH
  comment=''
  AND read_repair_chance=0
  AND dclocal_read_repair_chance=0.1
  AND gc_grace_seconds=864000
  AND bloom_filter_fp_chance=0.01;


  
  CREATE TABLE "TransactionDetails" (
    id bigint,
    amount double,
    "cardNumber" text,
    cardtype text,
    country text,
    day int,
    hour int,
    merchant text,
    min int,
    month int,
    status text,
    "transactionId" text,
    year int,
    txndate text,
    txntimestamp timestamp,
    PRIMARY KEY (id)
) WITH
  comment=''
  AND read_repair_chance=0
  AND dclocal_read_repair_chance=0.1
  AND gc_grace_seconds=864000
  AND bloom_filter_fp_chance=0.01;

  
  
  
  
CREATE TABLE "HourlyTransactionRollUpPerCountry" (
    id bigint,
    country text,
    day int,
    hour int,
    min int,
    month int,
	year int,
    totalcount int,
    trxntimestamp timestamp,
    totalsum double,
   PRIMARY KEY ((hour, country), id)
) WITH
  comment=''
  AND read_repair_chance=0
  AND dclocal_read_repair_chance=0.1
  AND gc_grace_seconds=864000
  AND bloom_filter_fp_chance=0.01;

  
  
  
CREATE TABLE "HourlyTransactionRollUpPerCardType" (
    id bigint,
    cardtype text,
    day int,
    hour int,
    min int,
    month int,
    totalcount int,
    totalsum double,
    trxntimestamp timestamp,
    year int,
    PRIMARY KEY ((hour, cardtype), id)
) WITH
  comment=''
  AND read_repair_chance=0
  AND dclocal_read_repair_chance=0.1
  AND gc_grace_seconds=864000
  AND bloom_filter_fp_chance=0.01;



CREATE TABLE "BatchHourlyTransactionRollUpPerCardType" (
    id bigint,
    cardtype text,
    day int,
    hour int,
    min int,
    month int,
    totalcount int,
    totalsum double,
    trxntimestamp timestamp,
    year int,
    PRIMARY KEY ((hour, cardtype), id)
) WITH
  comment=''
  AND read_repair_chance=0
  AND dclocal_read_repair_chance=0.1
  AND gc_grace_seconds=864000
  AND bloom_filter_fp_chance=0.01;

 CREATE TABLE "BatchHourlyTransactionRollUpPerCountry" (
    id bigint,
    country text,
    day int,
    hour int,
    min int,
    month int,
	year int,
    totalcount int,
    tnxtimestamp timestamp,
    totalsum double,
   PRIMARY KEY ((hour, country), id)
) WITH
  comment=''
  AND read_repair_chance=0
  AND dclocal_read_repair_chance=0.1
  AND gc_grace_seconds=864000
  AND bloom_filter_fp_chance=0.01;

 CREATE TABLE "BatchHourlyTransactionRollUpPerMerchant" (
    id bigint,
    day int,
    hour int,
    merchant text,
    min int,
    month int,
    totalcount int,
    totalsum double,
    trxntimestamp timestamp,
    year int,
   PRIMARY KEY ((hour, merchant), id)
) WITH
  comment=''
  AND read_repair_chance=0
  AND dclocal_read_repair_chance=0.1
  AND gc_grace_seconds=864000
  AND bloom_filter_fp_chance=0.01;