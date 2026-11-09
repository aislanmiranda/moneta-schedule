# moneta-schedule

Worker do **Moneta** que executa as **rotinas** criadas no app: verifica os módulos nos horários
configurados e gera os lembretes (contas que vencem, remédios pendentes, compromissos do dia).

> **Visão geral, imagens e como rodar o projeto completo:**
> [moneta-frontend](https://github.com/aislanmiranda/moneta-frontend) — lá está o
> `docker-compose.yml` que sobe banco, API, scheduler e PWA.

## O que tem aqui

.NET 10 Worker Service + Quartz.NET:

- **`ScheduleSyncJob`** (a cada 30s) mantém um trigger Quartz por rotina ativa, de acordo com o
  cron salvo no banco, e atende o "Executar agora" do app.
- **Jobs de lembrete** (Financeiro, Vida Saudável, Agenda) avaliam o módulo da rotina, gravam as
  notificações e avisam a API via `pg_notify` — que repassa em tempo real para o usuário.
- **`TimeControlOccurrenceGeneratorJob`** (a cada 10 min) gera as ocorrências do dia dos itens
  do Vida Saudável.

Detalhes em [`docs/JOBS.md`](docs/JOBS.md).

## Dependência do backend

O worker reaproveita os projetos `Domain`, `Application`, `Infra` e `Repository` do
[moneta-backend](https://github.com/aislanmiranda/moneta-backend) (mesmo modelo de dados, sem
duplicação). Clone os dois **lado a lado**:

```
moneta-backend/
moneta-schedule/   ← este repo
```

```bash
dotnet run   # usa a connection string de appsettings.json (localhost:5432)
```

Os horários usam o fuso da máquina (no Docker, a variável `TZ`, padrão `America/Sao_Paulo`).

## Licença

[MIT](LICENSE)
