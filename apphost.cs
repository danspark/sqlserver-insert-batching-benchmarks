#:sdk Aspire.AppHost.Sdk@13.5.3
#:property ManagePackageVersionsCentrally=false
#:package Aspire.Hosting.RabbitMQ@13.5.3
#:package Aspire.Hosting.SqlServer@13.5.3
#:property AspireUseCliBundle=true

var builder = DistributedApplication.CreateBuilder(args);

var sqlServer = builder.AddSqlServer("sqlserver")
    .WithImage("mssql/server", "2025-CU8-ubuntu-22.04")
    .WithImageRegistry("mcr.microsoft.com")
    .WithImageSHA256("2f9da673779dc5556d385164f6b1541d169ff1eeed97b9833ca0308e8628e683");
var database = sqlServer.AddDatabase("sqldb", "SqlInsertBenchmarks");

var rabbitMq = builder.AddRabbitMQ("rabbitmq")
    .WithManagementPlugin()
    .WithImage("library/rabbitmq", "4.3.5-management")
    .WithImageRegistry("docker.io")
    .WithImageSHA256("ffd1b50c522ad20172ffd6716a2f41db375c7269560c8f3fb9a694e210ef0852");

var workerReplicas = int.TryParse(builder.Configuration["Parameters:workerReplicas"], out var configuredReplicas)
    ? configuredReplicas
    : 1;
builder.AddProject("worker", "src/SqlBench.Worker/SqlBench.Worker.csproj")
    .WithArgs("--idle")
    .WithReplicas(workerReplicas)
    .WithReference(database)
    .WithReference(rabbitMq)
    .WithEnvironment("SQLBENCH_RABBIT_MANAGEMENT", rabbitMq.GetEndpoint("management"))
    .WaitFor(database)
    .WaitFor(rabbitMq);

builder.AddProject("controller", "src/SqlBench.Controller/SqlBench.Controller.csproj")
    .WithArgs("--idle")
    .WithReference(database)
    .WithReference(rabbitMq)
    .WithEnvironment("SQLBENCH_RABBIT_MANAGEMENT", rabbitMq.GetEndpoint("management"))
    .WaitFor(database)
    .WaitFor(rabbitMq);

builder.Build().Run();
