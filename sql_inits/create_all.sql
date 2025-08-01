--DROP DATABASE IF EXISTS BIT;
--CREATE DATABASE BIT;

-- Создайте базу данных если её нет
CREATE DATABASE IF NOT EXISTS BIT;


CREATE TABLE BIT.LGP
(
    DateTime            DateTime('UTC'),
    TransactionStatus   LowCardinality(String),
    TransactionDate     Nullable(DateTime('UTC')),
    TransactionNumber   UInt64,
    User                UInt32,        -- <— было String, стало UInt32
    Computer            UInt32,
    Application         UInt32,
    Connection          UInt32,
    Event               UInt32,
    Severity            LowCardinality(String),
    Comment             String,
    Data                String,
    DataPresentation    String,
    Server              UInt32,
    MainPort            UInt16,
    Metadata            UInt16,
    AddPort             UInt16,
    Session             UInt32,
    db_uid              LowCardinality(String),
    host                LowCardinality(String),
    FileName            String,
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(DateTime)
ORDER BY (DateTime, TransactionNumber);


CREATE TABLE BIT.LGF
(
    db_uid   LowCardinality(String),
    id       UInt32,
    typeId   UInt32,
    uuid     String,
    value    String
)
ENGINE = ReplacingMergeTree()
ORDER BY (db_uid, typeId, id);

CREATE TABLE BIT.IBASE
(
    ID   LowCardinality(String),
    File       String,
    Ref   String,
    Srvr     String,
    title    String
)
ENGINE = TinyLog();

CREATE VIEW BIT.JOURNAL_REG AS
SELECT
    p.DateTime,
    p.TransactionStatus,
    p.TransactionDate,
    p.TransactionNumber,

    -- User
    p.User               AS UserID,
    u.value              AS User,
    u.uuid               AS UserUuid,

    -- Computer
    p.Computer           AS ComputerID,
    c.value              AS Computer,
    c.uuid               AS ComputerUuid,

    -- Application
    p.Application        AS ApplicationID,
    a.value              AS Application,
    a.uuid               AS ApplicationUuid,

    -- Event
    p.Event              AS EventID,
    e.value              AS Event,
    e.uuid               AS EventUuid,

    -- Metadata
    p.Metadata           AS MetadataID,
    m.value              AS Metadata,
    m.uuid               AS MetadataUuid,

    -- Server
    p.Server             AS ServerID,
    s.value              AS Server,
    s.uuid               AS ServerUuid,

    -- MainPort
    p.MainPort           AS MainPortID,
    mp.value             AS MainPort,
    mp.uuid              AS MainPortUuid,

    -- AddPort
    p.AddPort            AS AddPortID,
    ap.value             AS AddPort,
    ap.uuid              AS AddPortUuid,

    -- остальные поля из lgp
    p.Connection,
    p.Severity,
    p.Comment,
    p.Data,
    p.DataPresentation,
    p.Session,
    p.db_uid as db_uid,
    p.host,
    p.FileName,

    -- поля из IBASE
    ib.File    AS IBASE_File,
    ib.Ref     AS IBASE_Ref,
    ib.Srvr    AS IBASE_Srvr,
    ib.title   AS IBASE_Title

FROM BIT.LGP AS p

-- обогащение по lgf (typeId=1..8)
LEFT ANY JOIN BIT.LGF AS u  ON p.db_uid = u.db_uid AND u.typeId = 1 AND p.User        = u.id
LEFT ANY JOIN BIT.LGF AS c  ON p.db_uid = c.db_uid AND c.typeId = 2 AND p.Computer    = c.id
LEFT ANY JOIN BIT.LGF AS a  ON p.db_uid = a.db_uid AND a.typeId = 3 AND p.Application = a.id
LEFT ANY JOIN BIT.LGF AS e  ON p.db_uid = e.db_uid AND e.typeId = 4 AND p.Event       = e.id
LEFT ANY JOIN BIT.LGF AS m  ON p.db_uid = m.db_uid AND m.typeId = 5 AND p.Metadata    = m.id
LEFT ANY JOIN BIT.LGF AS s  ON p.db_uid = s.db_uid AND s.typeId = 6 AND p.Server      = s.id
LEFT ANY JOIN BIT.LGF AS mp ON p.db_uid = mp.db_uid AND mp.typeId= 7 AND p.MainPort    = mp.id
LEFT ANY JOIN BIT.LGF AS ap ON p.db_uid = ap.db_uid AND ap.typeId= 8 AND p.AddPort     = ap.id

-- ваша новая таблица IBASE
LEFT ANY JOIN BIT.IBASE AS ib
  ON ib.ID = p.db_uid
;


CREATE TABLE BIT.REG1
(
    `DateTime` DateTime('UTC') CODEC(Delta(4), LZ4),
    `TransactionStatus` LowCardinality(String),
    `TransactionDate` DateTime('UTC') CODEC(Delta(4), LZ4),
    `TransactionNumber` UInt64 CODEC(Delta(4), LZ4),
    `UserUuid` String CODEC(LZ4),
    `User` String CODEC(ZSTD(9)),
    `Computer` String CODEC(ZSTD(9)),
    `Application` LowCardinality(String),
    `Connection` UInt32 CODEC(DoubleDelta, LZ4),
    `Event` LowCardinality(String),
    `Severity` LowCardinality(String),
    `Comment` String CODEC(ZSTD(19)),
    `MetadataUuid` String CODEC(LZ4),
    `Metadata` String CODEC(ZSTD(19)),
    `Data` String CODEC(ZSTD(19)),
    `DataPresentation` String CODEC(ZSTD(19)),
    `Server` LowCardinality(String),
    `MainPort` UInt16 CODEC(DoubleDelta, LZ4),
    `AddPort` UInt16 CODEC(DoubleDelta, LZ4),
    `Session` UInt32 CODEC(DoubleDelta, LZ4),
    `db_uid` LowCardinality(String),
    `host` LowCardinality(String),
    `FileName` String CODEC(ZSTD(9)),
    `date` Date MATERIALIZED toDate(DateTime)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(DateTime)
ORDER BY (DateTime, Severity, Event, TransactionNumber)
COMMENT 'Журнал регистрации событий 1С';



CREATE VIEW BIT.JOURNAL_REG_WITH_OLD_DATA AS

select 
  DateTime,
  TransactionStatus,
  TransactionDate,
  TransactionNumber,
  
  User,
  UserUuid,
  
  Computer,
  Application,
  Event,
  Metadata,
  MetadataUuid,
  Server,
  MainPort,
  AddPort,
  
  Connection,
  Severity,
  Comment,
  Data,
  DataPresentation,
  Session,
  db_uid,
  host,
  FileName,
  
  IBASE_File,
  IBASE_Ref,
  IBASE_Srvr,
  IBASE_Title
  
from BIT.JOURNAL_REG

UNION ALL

select
  DateTime,
  TransactionStatus,
  TransactionDate,
  TransactionNumber,
  
  User,
  UserUuid,
  
  Computer,
  Application,
  Event,
  Metadata,
  MetadataUuid,
  Server,
  toString(MainPort),
  toString(AddPort),
  
  Connection,
  Severity,
  Comment,
  Data,
  DataPresentation,
  Session,
  db_uid,
  host,
  FileName,
  
  ib2.File    AS IBASE_File,
  ib2.Ref     AS IBASE_Ref,
  ib2.Srvr    AS IBASE_Srvr,
  ib2.title   AS IBASE_Title
  
from BIT.REG1 as jr LEFT ANY JOIN BIT.IBASE AS ib2
  ON ib2.ID = jr.db_uid;