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
    FilePath            String,
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
ENGINE = TinyLog();

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
    u.uuid               AS UserUUID,

    -- Computer
    p.Computer           AS ComputerID,
    c.value              AS Computer,
    c.uuid               AS ComputerUUID,

    -- Application
    p.Application        AS ApplicationID,
    a.value              AS Application,
    a.uuid               AS ApplicationUUID,

    -- Event
    p.Event              AS EventID,
    e.value              AS Event,
    e.uuid               AS EventUUID,

    -- Metadata
    p.Metadata           AS MetadataID,
    m.value              AS Metadata,
    m.uuid               AS MetadataUUID,

    -- Server
    p.Server             AS ServerID,
    s.value              AS Server,
    s.uuid               AS ServerUUID,

    -- MainPort
    p.MainPort           AS MainPortID,
    mp.value             AS MainPort,
    mp.uuid              AS MainPortUUID,

    -- AddPort
    p.AddPort            AS AddPortID,
    ap.value             AS AddPort,
    ap.uuid              AS AddPortUUID,

    -- остальные поля из lgp
    p.Connection,
    p.Severity,
    p.Comment,
    p.Data,
    p.DataPresentation,
    p.Session,
    p.db_uid,
    p.host,
    p.FileName,
    p.FilePath,

    -- поля из IBASE
    ib.File    AS IBASE_File,
    ib.Ref     AS IBASE_Ref,
    ib.Srvr    AS IBASE_Srvr,
    ib.title   AS IBASE_Title

FROM BIT.LGP AS p

-- обогащение по lgf (typeId=1..8)
LEFT JOIN BIT.LGF AS u  ON p.db_uid = u.db_uid AND u.typeId = 1 AND p.User        = u.id
LEFT JOIN BIT.LGF AS c  ON p.db_uid = c.db_uid AND c.typeId = 2 AND p.Computer    = c.id
LEFT JOIN BIT.LGF AS a  ON p.db_uid = a.db_uid AND a.typeId = 3 AND p.Application = a.id
LEFT JOIN BIT.LGF AS e  ON p.db_uid = e.db_uid AND e.typeId = 4 AND p.Event       = e.id
LEFT JOIN BIT.LGF AS m  ON p.db_uid = m.db_uid AND m.typeId = 5 AND p.Metadata    = m.id
LEFT JOIN BIT.LGF AS s  ON p.db_uid = s.db_uid AND s.typeId = 6 AND p.Server      = s.id
LEFT JOIN BIT.LGF AS mp ON p.db_uid = mp.db_uid AND mp.typeId= 7 AND p.MainPort    = mp.id
LEFT JOIN BIT.LGF AS ap ON p.db_uid = ap.db_uid AND ap.typeId= 8 AND p.AddPort     = ap.id

-- ваша новая таблица IBASE
LEFT JOIN BIT.IBASE AS ib
  ON ib.ID = p.db_uid
;