# Product Audit — SQL Schema MCP

**Datum:** 2026-07-06
**Reviewer-rol:** kritische externe reviewer — "zou een security-bewuste lead dev dit durven installeren?"
**Scope:** volledige repo, alleen gelezen. Geen code gewijzigd.

---

## 1. Samenvatting

**Oordeel: nee — een security-bewuste lead developer installeert dit nu niet.** Niet omdat de code slecht is (de schema-tools zijn netjes geparametriseerd en goed gestructureerd), maar omdat het kernbeloftevlak niet klopt. README en `CLAUDE.md` beloven letterlijk *"No query execution. No data access. Schema and metadata only."* — maar de server levert een `execute_query`-tool (willekeurige SELECT) plus vier `DataTools` die échte rijdata, min/max-waarden en voorbeeldrijen teruggeven. De belangrijkste verkoopbelofte ("we raken je data niet aan") is in de verscheepte code onwaar. Dat is precies het punt waarop een reviewer afhaakt.

Daar bovenop: read-only is *niet* technisch afgedwongen. `SqlCommandGuard` is een keyword-denylist die te omzeilen is (`SELECT ... INTO`, `OPENQUERY`/`OPENROWSET`). De enige echte verdediging is een read-only databaselogin — en die eis staat nergens gedocumenteerd, wordt niet bij startup geverifieerd, en de voorbeeldconfig gebruikt `Trusted_Connection`/AAD zonder woord over minimale rechten. Verder: foutmeldingen lekken rauwe `SqlException`-tekst (servernaam, login), er is geen enkele audit-logging, en er zijn nul tests.

**Afstand tot verkoopbaar:** grofweg **6–8 avonden** tot een verdedigbare eerste externe tester — mits eerst een principebesluit valt over de data-toegang (zie blocker B1). De schema-kern zelf is dichtbij; de trust-laag eromheen ontbreekt.

---

## 2. Bevindingen

### 2a. Security & vertrouwen (zwaarst wegend)

| # | Bevinding | Severity | Effort |
|---|-----------|----------|--------|
| S1 | **Product-belofte klopt niet met de code.** README/CLAUDE.md: "no query execution, no data access, schema only". Werkelijkheid: `execute_query` (vrije SELECT) + `SampleTableData`/`AnalyzeColumnDistribution`/`FindDuplicateRows`/`FindNullableColumnsWithNoNulls` geven echte rijwaarden, min/max en samples terug. Kolom-min/max lekt direct PII (laagste/hoogste e-mail, naam, geboortedatum). | blocker | M |
| S2 | **Read-only is niet technisch afgedwongen — alleen conventie.** `SqlCommandGuard` (`Data/SqlCommandGuard.cs`) is een regex-denylist op keywords. Bypass-vectoren die géén verboden keyword bevatten: `SELECT * INTO nieuwe_tabel FROM x` (maakt een tabel — DDL/write), `SELECT * FROM OPENQUERY(linked, 'DELETE ...')` en `OPENROWSET(...)` (arbitraire remote write). De denylist mist dus schrijfpaden. | blocker | M |
| S3 | **Geen gedocumenteerde/afgedwongen read-only login.** De enige echte verdediging tegen S2 is DB-rechten, maar: geen eis in docs, geen startup-check op de rechten van het account, voorbeeldconfig gebruikt `Trusted_Connection=true` en AAD Default (vaak ruime rechten). Een gebruiker die dit installeert geeft de agent standaard te veel. | blocker | S |
| S4 | **Foutmeldingen lekken infra-details.** Overal `return $"ERROR: {ex.Message}"`. Rauwe `SqlException` bevat servernaam, databasenaam, loginnaam, netwerkdetails — die belanden in de context van het model en in logs. | belangrijk | S |
| S5 | **Geen enkele audit-logging.** Nergens een `ILogger`-aanroep in `Tools/` of `Data/`. Niet vast te stellen welke tool wanneer welke database raakte. Voor een team-product is auditbaarheid een koopargument; nu ontbreekt het volledig. | belangrijk | M |
| S6 | **HTTP-modus heeft nep-OAuth zonder enige validatie.** `Program.cs` implementeert `/oauth/*` endpoints die elk token accepteren ("no tokens are validated"). Bindt op `localhost`, dus lokaal acceptabel — maar het is expliciet géén team-transport en dat moet hard gedocumenteerd blijven, anders zet iemand het achter een reverse proxy. | belangrijk | S |
| S7 | **Prompt-injection blast radius niet gedocumenteerd.** Een gecompromitteerde/geïnjecteerde agent kan via `execute_query` + DataTools volledige tabellen uitlezen (tot 500 rijen per call, herhaalbaar) en via S2 mogelijk schrijven. Er is geen threat-model of dataclassificatie-waarschuwing. | belangrijk | S |

### 2b. Schema-coverage

| # | Bevinding | Severity | Effort |
|---|-----------|----------|--------|
| C1 | **Coverage is juist een sterk punt.** Tabellen/kolommen/types, PK, FK+relaties, indexen, views, procs, functions, triggers (DML+DDL), synonyms, check-constraints, MS_Description — allemaal aanwezig en netjes. Ver boven wat `CLAUDE.md` als minimum vroeg. | n.v.t. | — |
| C2 | **Cross-database bestaat en werkt** (`CompareTools`: tables/procs/views + kolom-diff). Killer feature voor Strangler Fig. | n.v.t. | — |
| C3 | **Geen echte cross-DB relatie-inferentie.** Compare legt objecten naast elkaar, maar leidt geen legacy↔modern kolomrelaties af (bv. "deze tabel is de gemigreerde variant van gene"). Productkans, geen gebrek. | nice-to-have | L |
| C4 | **Response-compactheid deels onbewaakt.** `GetTableSchema` is één regel per kolom (prima). Maar `ListTables` en vooral `GenerateDatabaseSummary` hebben geen paging/limiet — op 1000+ tabellen wordt dat een grote, dure response. Geen samenvattingsniveau of `top`-parameter. | belangrijk | M |

### 2c. Robuustheid

| # | Bevinding | Severity | Effort |
|---|-----------|----------|--------|
| R1 | **Foutafhandeling is consistent maar lekt (zie S4).** Onbereikbare DB/timeout/permissie-fout wordt netjes als string teruggegeven i.p.v. crash — goed. Alleen de inhoud is te breed. | belangrijk | S |
| R2 | **Timeouts aanwezig maar inconsistent.** `execute_query` 30s, `FindDuplicateRows`/`FindNullableColumnsWithNoNulls` 120s, de rest gebruikt de default (30s). Geen globale, configureerbare timeout. | nice-to-have | S |
| R3 | **Geen schema-caching.** Elke call opent een nieuwe connectie en query't opnieuw. Voor een read-heavy analyse-sessie is dat veel round-trips, maar het vermijdt cache-invalidatie-problemen bij migraties die de agent zelf draait. Bewuste trade-off; prima voor nu. | nice-to-have | M |
| R4 | **Concurrency op `constraints.json` is niet gelockt.** Gedocumenteerd in README (twee stdio-sessies kunnen elkaar overschrijven). Reëel maar klein; write is zeldzaam. | nice-to-have | S |
| R5 | **`SqlCommandGuard.AssertReadOnly` wordt óók op twee hardcoded, constante queries toegepast** (`ListDdlTriggers`, `GetDdlTriggerDefinition` in `SchemaQueries`). Zinloos daar (de SQL is een compile-time constante) — verwarrend, suggereert onbegrip over waar de guard waarde heeft. | nice-to-have | S |

### 2d. Generalisatie & DX

| # | Bevinding | Severity | Effort |
|---|-----------|----------|--------|
| G1 | **Weinig Datalake2-hardcoding.** DB-aliassen (`poc`/`azure`) zijn gewoon config-keys, niet hardcoded. Enige aanname: de staging-regex `_YYYYMMDD_HHMMSS` in `SqlQueryBase`/`PipelineTools` is ETL-specifiek. Prima als opt-in feature, maar documenteer dat PipelineTools die naamconventie aanneemt. | belangrijk | S |
| G2 | **100% SQL Server-gekoppeld.** Alle queries gebruiken `sys.*` / `INFORMATION_SCHEMA` / T-SQL-specifieke constructies (`FOR XML PATH`, `QUOTENAME`, `dm_*`). PostgreSQL-abstractie vergt een provider-laag achter elke query-methode — schatting: **L** (grote klus, ~alle 8 query-klassen raken). Niet bouwen; als bewuste v2-scope parkeren. | nice-to-have | L |
| G3 | **Installatiepad matcht de distributie-ambitie niet.** `CLAUDE.md` mikt op `dotnet tool install -g`, maar README zegt "clone de repo + `dotnet run`". Geen `<PackAsTool>`, geen tool-manifest, geen NuGet-metadata. De 10-minuten-onboarding bestaat nog niet. | belangrijk | M |
| G4 | **`Sdk.Web` voor iedereen.** De csproj is `Microsoft.NET.Sdk.Web` (nodig voor HTTP-modus), waardoor ook stdio-only gebruikers de hele ASP.NET-stack binnenhalen. Bij packaging als tool overwegen HTTP-modus te scheiden of achter een optionele build te zetten. | nice-to-have | M |

### 2e. Productization

| # | Bevinding | Severity | Effort |
|---|-----------|----------|--------|
| P1 | **Geen tests.** Nul testprojecten, geen fixture-DB. Voor een product waar "read-only" de kernbelofte is, is het ontbreken van een test die bewijst dat de guard schrijfpogingen blokkeert (en de bypasses van S2 vangt) op zichzelf een blocker voor extern vertrouwen. | blocker | M |
| P2 | **Geen CI/publish-pipeline.** Geen Azure DevOps YAML, geen versioning, geen NuGet-publish. Nodig voor de open-core distributie. | belangrijk | M |
| P3 | **Open-core scheidslijn nog niet getrokken in code.** Alle tools zitten in één assembly, één DI-registratie. Voor "gratis single-DB kern / betaald multi-DB + diff + audit" is er nog geen edition-scheiding of feature-flag. | nice-to-have | M |

---

## 3. Quick wins (< 1 avond, directe waarde)

1. **S4/R1 — foutmeldingen saneren.** Vervang `return $"ERROR: {ex.Message}"` door een generieke boodschap ("query failed against '{database}': <categorie>") en log de details alleen via `ILogger`. Eén helper in `SqlQueryBase`, overal toepassen. Grootste vertrouwenswinst per uur.
2. **S3 — read-only login documenteren.** README-sectie "Security posture" + `appsettings.example.json` commentaar dat een dedicated read-only login vereist is, met een `CREATE LOGIN ... db_datareader`-snippet. Kost minuten, verschuift de belofte van "vertrouw de code" naar "vertrouw de rechten".
3. **R5 — zinloze guard-calls verwijderen** in `SchemaQueries.ListDdlTriggers`/`GetDdlTriggerDefinition`. Kleine opschoning die verwarring wegneemt.
4. **G1 — PipelineTools-aanname documenteren** (de `_YYYYMMDD_HHMMSS`-conventie) zodat een nieuw team niet verrast wordt.
5. **S6 — HTTP-modus expliciet als "lokaal, geen auth" labelen** boven de OAuth-endpoints en in de README (staat er half; maak het een waarschuwing).

---

## 4. v1.0-blockers (minimale set vóór eerste externe tester)

- **B1 (S1) — Besluit en herstel de data-toegangspositie.** Dit is *de* beslissing. Drie opties (zie §6). Zonder dit klopt het productverhaal niet en haakt elke security-reviewer af.
- **B2 (S2+S3) — Read-only technisch verdedigbaar maken.** Minimaal: startup-check die weigert te starten als het login schrijfrechten heeft (of luid waarschuwt), plus de guard uitbreiden/vervangen zodat `SELECT INTO` en `OPENQUERY`/`OPENROWSET` niet doorglippen. De denylist mag nooit de *primaire* verdediging zijn — maak dat expliciet.
- **B3 (P1) — Tests die de read-only-belofte bewijzen.** Een testsuite die aantoont dat elke bekende write/bypass geblokkeerd wordt, plus happy-path tests voor de schema-tools tegen een fixture-DB.
- **B4 (S4) — Geen infra-lek in foutmeldingen.** (Overlapt met quick win 1; hier als harde eis.)

---

## 5. Later (bewust geparkeerd)

- **G2 — PostgreSQL-abstractie.** Grote klus (L), pas relevant als er marktvraag buiten SQL Server is. Parkeren tot na eerste betalende SQL Server-klant.
- **C3 — cross-DB relatie-inferentie.** Sterke feature, maar bovenop een werkend en vertrouwd fundament. v1.1+.
- **P3 — open-core edition-scheiding.** Pas zinvol als de kern verkoopbaar is; nu premature optimalisatie.
- **R3 — schema-caching.** Bewuste trade-off; huidige gedrag is correct, alleen minder snel. Alleen aanpakken bij aantoonbare performanceklachten.
- **S5/G4 — audit-logging infra + `Sdk.Web`-splitsing** kunnen mee met de packaging-slag (P2), geen aparte prioriteit.

---

## 6. Voorstel per blocker/quick win (aanpak, files, valkuilen — niet uitgevoerd)

### B1 — Data-toegangspositie (het kernbesluit)
Drie routes, kies er één en maak README/CLAUDE.md consistent:

- **Route A — "Schema only, zoals beloofd" (aanbevolen voor de security-pitch).** Verwijder `QueryTools`/`QueryQueries` en `DataTools`/`DataQueries` uit de default-build; zet ze achter een expliciete opt-in flag (`--allow-data` / config `AllowDataAccess: true`) die standaard uit staat. Dan klopt de belofte weer en is data-toegang een bewuste, gelogde keuze.
  - Files: `Program.cs` (conditionele `WithTools<>`), `SqlServerOptions` (nieuwe flag), README/CLAUDE.md.
  - Valkuil: de min/max in `AnalyzeColumnDistribution` blijft PII lekken zodra data-modus aan staat — documenteer dat.
- **Route B — "Data-tools zijn een feature, herschrijf de belofte."** Houd de tools, maar verander README/CLAUDE.md naar "read-only, inclusief begrensde datasampling" en maak de risico's expliciet (PII in samples/min-max). Eerlijker dan nu, maar minder sterke security-pitch.
- **Route C — hybride:** Route A als default + Route B als gedocumenteerde "power mode". Meeste werk, beste verkoopverhaal.

### B2 — Read-only afdwinging
- **Startup-check:** query bij opstart `HAS_PERMS_BY_NAME` / `fn_my_permissions` of probeer een gecontroleerde no-op write in een transactie die altijd rollback't; weiger te starten (of log CRITICAL) als schrijven lukt.
- **Guard:** vervang de pure denylist door (a) een parser/allowlist die alleen `SELECT`/`WITH` als top-statement toestaat, en (b) expliciete blokkade van `INTO`, `OPENQUERY`, `OPENROWSET`, `OPENDATASOURCE`, `WAITFOR`. Files: `Data/SqlCommandGuard.cs`, `Data/QueryQueries.cs`.
- **Valkuil:** een naïeve allowlist blokkeert legitieme CTE's en `SELECT` met subqueries; test breed. En blijf communiceren dat de guard secundair is — de login is primair.

### B3 — Tests
- Nieuw `SqlSchemaMcp.Tests`-project (xUnit + FluentAssertions per huisstijl). Guard-tests hebben geen DB nodig (pure functie). Schema-tool-tests tegen een lokale SQL Server-fixture of container (Testcontainers).
- Valkuil: fixture-DB opzetten is de echte kost; guard-tests zijn goedkoop en leveren de meeste vertrouwenswinst — begin daar.

### B4 / Quick win 1 — Foutsanitisatie
- Helper `protected string SafeError(string database, Exception ex)` in `SqlQueryBase`; log volledig via geïnjecteerde `ILogger`, retourneer generieke tekst. Files: alle 8 `*Queries.cs`.
- Valkuil: `UnknownDatabase` mag de geldige namen blijven tonen (dat is bedoeld en niet gevoelig).

### Quick win 2/3/4/5
- Puur docs + een kleine code-verwijdering (R5). Laag risico, geen architecturale wijziging. R5 valt onder "commented-out/zinloze code weg" — maar omdat het een guard-call betreft: bevestig eerst dat er echt geen dynamische SQL binnensluipt in die twee methodes (dat is zo — de SQL is constant).

---

## 7. Bundel-notities (met Code Intelligence MCP)

- **Gedeelde config-vorm:** beide MCP's hebben een lijst named targets (databases hier, codebases daar). Eén gedeeld config-schema + één `dotnet tool` die beide servers levert is een geloofwaardige "AI snapt je hele .NET-stack"-pitch.
- **Gedeelde trust-laag:** de security-bevindingen hier (read-only afdwinging, audit-logging, foutsanitisatie, threat-model) zijn precies wat een Code Intelligence MCP óók moet kunnen aantonen. Eén gezamenlijke "security posture"-doc en één audit-logging-mechanisme herbruiken loont.
- **Gezamenlijke killer-use-case:** agent leest code (CI-MCP) + schema (dit) → cross-reference van ORM-modellen tegen echte DB-schema, of Classic ASP-queries tegen de tabellen die ze raken. Dat is voor de Strangler Fig-doelgroep sterker dan elk product apart.
- **Beslissing parkeren** tot de Code Intelligence-audit klaar is, zoals afgesproken.

---

## 8. Wrap-up

**Beslissingen die nu genomen moeten worden:**
1. B1 — welke data-toegangsroute (A/B/C)? Dit stuurt al het andere.
2. Distributie: blijft het "clone + dotnet run" of gaan we echt naar `dotnet tool` (G3)?

**Open vragen:**
- Voor welke SQL Server-versies moet v1.0 garanderen te werken? (Sommige `sys.dm_*`/DMV-queries en `dm_db_index_physical_stats` vragen bepaalde permissies/edities.)
- Is PostgreSQL (Coach-app) een v1-eis of bewust v2? (Nu als v2 aangenomen.)
- Wat is de gevoeligheidsklasse van de doeldatabases — bepaalt hoe hard B1/B2 moeten.

**Aannames gemaakt tijdens deze audit** (conform werkafspraak):
- De schema-tools zijn de kern; `execute_query` + DataTools zijn later toegevoegd en botsen bewust of onbewust met de oorspronkelijke "schema only"-scope. Ik heb dit als het centrale spanningsveld behandeld.
- PostgreSQL is v2-scope.
- Doelgroep = security-bewuste .NET-teams met gevoelige legacy-data (uit de context van het audit-document).

**Startpunt volgende sessie:** neem het B1-besluit, en begin daarna met de goedkoopste hoge-impact-acties: quick win 1 (foutsanitisatie) + B3 guard-tests. Die twee samen tillen het vertrouwensniveau het snelst omhoog en zijn onafhankelijk van de B1-routekeuze.
