FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY SqlSchemaMcp.csproj ./
RUN dotnet restore SqlSchemaMcp.csproj
COPY . .
RUN dotnet publish SqlSchemaMcp.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

# curl is required for the HEALTHCHECK below; the base image does not ship it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/publish .
COPY appsettings.example.json ./appsettings.json

# Non-secret defaults only. Real connection strings and the port are injected as
# SQLMCP_-prefixed environment variables at `docker run` / compose time (see
# docker-compose.yml and .env.example) — never baked into the image.
ENV SQLMCP_Mcp__BindAddress=0.0.0.0

EXPOSE 5101

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f "http://localhost:${SQLMCP_Mcp__Port:-5101}/" || exit 1

ENTRYPOINT ["dotnet", "SqlSchemaMcp.dll", "--", "--sse"]
