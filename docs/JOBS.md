# Jobs do moneta-schedule

Worker service separado do `backend/`, dedicado a jobs recorrentes (Quartz.NET). Executa as
**rotinas** criadas pelo usuário na tela "Rotinas" do app — hoje, lembretes dos módulos
(Financeiro, Vida Saudável, Agenda).

## Por que um processo separado

O agendamento roda num worker próprio (`Microsoft.Extensions.Hosting` + Quartz), não
dentro da `AgentOffice.Api`, para que reiniciar/atualizar a API não interrompa jobs em
andamento e vice-versa. Ele referencia diretamente os projetos `Domain`, `Application`,
`Infra` e `Repository` do `backend/` — reaproveita o mesmo `AppDbContext` e as mesmas
entidades, sem duplicar o modelo de dados nem expor endpoints HTTP próprios.

## Como rodar

```bash
# precisa do Postgres de pé (docker compose up -d postgres na raiz)
dotnet run --project schedule
```

Ou tudo junto, na ordem certa: `docker compose up -d` na raiz.

Cada `Routine` (criada em "Rotinas" → "Nova rotina": título, módulo, frequência e, para
módulos de vencimento, regras de lembrete) carrega o próprio `CronExpression`.
`ScheduleSyncJob` lê as rotinas do Postgres a cada 30s e cria/atualiza/remove o trigger
correspondente — mudar a frequência, pausar ou excluir uma rotina na UI tem efeito aqui em até
30s, sem reiniciar o worker. Uma rotina nova roda uma vez logo após ser criada (a API já a
grava com `TriggerRequestedAt` preenchido), sem esperar o primeiro tick do cron.

## Configuração (`appsettings.json`)

```json
{
  "ConnectionStrings": { "AgentOffice": "..." }
}
```

Nada específico de feature aqui — o que cada rotina faz (tipo, frequência, regras de lembrete)
vem da própria `Routine` no banco, não de config do processo.

## Jobs

| Job | Arquivo | O que faz |
|---|---|---|
| `ScheduleSyncJob` | `Jobs/ScheduleSyncJob.cs` | Roda a cada 30s. Mantém o dicionário `RoutineType → JobKey`; para cada `Routine`, cria/atualiza/remove o trigger `routine-{id}-cron-trigger` conforme `Status` e `CronExpression`, grava `NextRunAt` de volta no banco (a UI mostra "próxima execução") e, se `TriggerRequestedAt` estiver preenchido, dispara a rotina na hora e limpa o campo ("Executar agora"). Também remove triggers de rotinas excluídas. Novos tipos de rotina entram adicionando uma linha em `JobKeyByType`. |
| `DateControlReminderJob` | `Jobs/DateControlReminderJob.cs` | Rotina de módulo de vencimento (ex.: Financeiro). Promove lançamentos vencidos a `Overdue` e avalia as regras de lembrete da rotina (antes do vencimento, no dia, repetir após vencer), agrupando em até 3 `Notification`s por execução. Detalhes em `backend/docs/features/financeiro-pessoal.md`. |
| `TimeControlReminderJob` | `Jobs/TimeControlReminderJob.cs` | Rotina de módulo por horário (ex.: Vida Saudável). Lembra das ocorrências pendentes cujo horário já passou, agrupadas por tipo. |
| `DateTimeControlReminderJob` | `Jobs/DateTimeControlReminderJob.cs` | Rotina de módulo de agenda. Lembra dos compromissos cujo horário já passou e ainda não foram concluídos. |
| `TimeControlOccurrenceGeneratorJob` | `Jobs/TimeControlOccurrenceGeneratorJob.cs` | Infraestrutura, não uma rotina: a cada 10 min gera as ocorrências do dia de cada item ativo dos módulos por horário. |

Os três jobs de lembrete recebem o id da rotina via `JobDataMap["RoutineId"]`, ficam restritos
ao `ModuleId` dela e registram cada execução em `RoutineExecutions` (é dali que sai a "última
execução" na UI). São registrados com `StoreDurably()` — existem sem trigger até o
`ScheduleSyncJob` criar um a partir do banco.

### "Executar agora"

Botão na lista de rotinas (`POST /api/routines/{id}/trigger-now`) que seta
`Routine.TriggerRequestedAt`. `ScheduleSyncJob` detecta isso no próximo poll (até 30s depois),
dispara a rotina via `scheduler.TriggerJob(...)` — fora do ciclo normal do cron, sem afetar o
próximo disparo agendado — e limpa o campo. A API só grava o pedido no banco; o worker decide o
que fazer com ele — nenhum dos dois processos chama o outro diretamente.

Novos jobs seguem o mesmo padrão: uma classe `IJob` com `[DisallowConcurrentExecution]`,
resolvendo `AppDbContext` via `IServiceScopeFactory` (nunca injetando o `DbContext`
diretamente no job, que vive por todo o processo), registrada em `Program.cs` com seu
próprio `JobKey`.

Depois de gravar cada `Notification`, os jobs de lembrete disparam
`pg_notify('notifications_channel', id)` — é o gatilho que o backend usa pra empurrar a
notificação em tempo real pro frontend via SignalR. Ver
`backend/docs/features/notificacoes-realtime.md`.

**Cuidado ao precisar do `IScheduler` dentro de um job** (caso do `ScheduleSyncJob`, que
cria/atualiza triggers de outro job): não injete `ISchedulerFactory` no construtor — o
host do Quartz.Extensions.Hosting usa schedulers nomeados/keyed na DI e isso quebra em
runtime (`OptionsValidationException` na subida). Use `context.Scheduler`, disponível
dentro de `Execute(IJobExecutionContext context, ...)`.
