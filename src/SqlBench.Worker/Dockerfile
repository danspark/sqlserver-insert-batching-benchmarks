FROM mcr.microsoft.com/dotnet/sdk:10.0.400@sha256:e1ffd2a92ae84c1291bc1b6887501f8af98e6331e7af6d4c8d37168c5e87a64c AS build
WORKDIR /source
COPY . .
RUN dotnet publish src/SqlBench.Worker/SqlBench.Worker.csproj -c Release -o /app --no-self-contained

FROM mcr.microsoft.com/dotnet/runtime:10.0.11@sha256:a365ce6a50b09176855d085c69da3fc1204a48432e36087e9a208f6e5860e235
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "SqlBench.Worker.dll"]
