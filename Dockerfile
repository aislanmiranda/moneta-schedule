# Build context: raiz deste repositório (moneta-schedule). O código do backend (Domain,
# Application, Infra, Repository) chega pelo contexto adicional "backend" — ver docker-compose:
#   build: { context: ../moneta-schedule, additional_contexts: { backend: ../moneta-backend } }
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY --from=backend src/ backend/src/
COPY . schedule/
RUN dotnet publish schedule/AgentOffice.Scheduler.csproj -c Release -o /app -p:BackendDir=../backend

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
# Npgsql probes for GSSAPI on startup; without this lib it logs a noisy (harmless) error.
RUN apt-get update && apt-get install -y --no-install-recommends libgssapi-krb5-2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app .
ENTRYPOINT ["dotnet", "AgentOffice.Scheduler.dll"]
