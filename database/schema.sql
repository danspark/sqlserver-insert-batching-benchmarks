SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.BenchmarkTarget', N'U') IS NOT NULL DROP TABLE dbo.BenchmarkTarget;
IF OBJECT_ID(N'dbo.ParentEntity', N'U') IS NOT NULL DROP TABLE dbo.ParentEntity;
IF OBJECT_ID(N'dbo.InsertBenchmarkRows', N'P') IS NOT NULL DROP PROCEDURE dbo.InsertBenchmarkRows;
IF TYPE_ID(N'dbo.BenchmarkRowType') IS NOT NULL DROP TYPE dbo.BenchmarkRowType;
GO

CREATE TABLE dbo.ParentEntity
(
    ParentId int NOT NULL CONSTRAINT PK_ParentEntity PRIMARY KEY,
    ExternalId uniqueidentifier NOT NULL,
    Category tinyint NOT NULL,
    Name nvarchar(80) NOT NULL
);
GO

;WITH source AS
(
    SELECT TOP (10000)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS number
    FROM sys.all_objects AS left_source
    CROSS JOIN sys.all_objects AS right_source
)
INSERT dbo.ParentEntity (ParentId, ExternalId, Category, Name)
SELECT
    number,
    CONVERT(uniqueidentifier, HASHBYTES('MD5', CONVERT(varchar(12), number))),
    number % 16,
    CONCAT(N'Parent ', number)
FROM source;
GO

CREATE TABLE dbo.BenchmarkTarget
(
    Id bigint IDENTITY(1, 1) NOT NULL CONSTRAINT PK_BenchmarkTarget PRIMARY KEY CLUSTERED,
    MessageId uniqueidentifier NOT NULL,
    ParentId int NOT NULL,
    CorrelationId uniqueidentifier NOT NULL,
    OccurredAt datetime2(3) NOT NULL,
    SequenceNo int NOT NULL,
    CounterValue bigint NOT NULL,
    Priority smallint NOT NULL,
    Amount decimal(18, 4) NOT NULL,
    IsActive bit NOT NULL,
    Code varchar(32) NOT NULL,
    Description nvarchar(128) NOT NULL,
    PayloadHash binary(16) NOT NULL,
    OptionalNote nvarchar(64) NULL,
    CONSTRAINT UQ_BenchmarkTarget_MessageId UNIQUE NONCLUSTERED (MessageId),
    CONSTRAINT FK_BenchmarkTarget_ParentEntity FOREIGN KEY (ParentId)
        REFERENCES dbo.ParentEntity (ParentId)
);
GO

CREATE INDEX IX_BenchmarkTarget_ParentId ON dbo.BenchmarkTarget (ParentId);
GO

CREATE TYPE dbo.BenchmarkRowType AS TABLE
(
    MessageId uniqueidentifier NOT NULL,
    ParentId int NOT NULL,
    CorrelationId uniqueidentifier NOT NULL,
    OccurredAt datetime2(3) NOT NULL,
    SequenceNo int NOT NULL,
    CounterValue bigint NOT NULL,
    Priority smallint NOT NULL,
    Amount decimal(18, 4) NOT NULL,
    IsActive bit NOT NULL,
    Code varchar(32) NOT NULL,
    Description nvarchar(128) NOT NULL,
    PayloadHash binary(16) NOT NULL,
    OptionalNote nvarchar(64) NULL
);
GO

CREATE PROCEDURE dbo.InsertBenchmarkRows
    @Rows dbo.BenchmarkRowType READONLY
AS
BEGIN
    SET NOCOUNT ON;

    INSERT dbo.BenchmarkTarget
    (
        MessageId, ParentId, CorrelationId, OccurredAt, SequenceNo,
        CounterValue, Priority, Amount, IsActive, Code, Description,
        PayloadHash, OptionalNote
    )
    SELECT
        MessageId, ParentId, CorrelationId, OccurredAt, SequenceNo,
        CounterValue, Priority, Amount, IsActive, Code, Description,
        PayloadHash, OptionalNote
    FROM @Rows;
END;
GO
