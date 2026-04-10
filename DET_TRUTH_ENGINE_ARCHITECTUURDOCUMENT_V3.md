# DET TRUTH ENGINE — ARCHITECTUURDOCUMENT V3
Voor project: IKBR Scalping Bot  
Status: Architectuuranker  
Doel: inhoudelijke waarheid, classificatie, risico en execution strikt scheiden, met expliciete ruimte voor markt-specifieke SL / TP / R-logica

---

## 1. DOEL EN FUNCTIE

De DET Truth Engine is de inhoudelijke besliskern van de scalpingbot.

Doel:  
de bot mag TradingView-alerts nooit behandelen alsof dat al trade-beslissingen zijn,  
maar moet zelf bepalen of een situatie inhoudelijk waar, valide en sterk genoeg is  
om als echte DET-tradekandidaat te bestaan.

Kernvraag:  
**“Is deze marktsituatie volgens DET-logica inhoudelijk echt goed genoeg om richting risico-allocatie en mogelijke execution te mogen gaan?”**

De Truth Engine beoordeelt:
- contextkwaliteit
- richtingslogica
- structuurvaliditeit
- triggerkwaliteit
- timing
- invalidatie-logica
- stop-logica
- targetlogica
- R-kwaliteit
- inhoudelijke classificatie

Belangrijke afbakening:  
de Truth Engine is géén transportlaag,  
géén brokerlaag,  
géén Pine-laag,  
géén risk allocator,  
en géén orderuitvoerder.

De Truth Engine is de inhoudelijke beoordelingslaag tussen observatie en downstream besluitvorming.

---

## 2. KERNPRINCIPE

Vaste hoofdregel:

Pine observeert.  
De bot interpreteert.  
De bot classificeert.  
De risk engine alloceert risico.  
De execution safety gate beslist of plaatsing technisch/operationeel mag.  
De order engine vertaalt logica naar broker-uitvoerbare orders.  
De broker voert alleen uit wat al bot-side is goedgekeurd.

Dus:

**TradingView / Pine**  
→ levert ruwe observaties en beperkte diagnostische hints

**FastAPI / webhook-laag**  
→ valideert payload technisch  
→ zet kandidaat non-blocking in queue

**Execution worker / bot core**  
→ draait DET Truth Engine  
→ bepaalt inhoudelijke waarheid  
→ bepaalt REJECT / SHADOW / A / A+  
→ levert afgeleide trade-logica aan downstream lagen

**Risk Engine**  
→ bepaalt alleen risicoallocatie voor A / A+

**Execution Safety Gate**  
→ bepaalt execution yes / no op basis van operationele en technische veiligheid

**Order Engine**  
→ vertaalt toegestaan tradeplan naar bracket-orderlogica en executable prijsniveaus

**IBKR**  
→ execution-only eindpunt

---

## 3. NIET-ONDERHANDELBARE ARCHITECTUURREGEL

**Pine is nooit DET-authority.**

Dat betekent concreet:
- Pine bepaalt niet of iets valid is
- Pine bepaalt niet of iets A of A+ is
- Pine bepaalt niet of execution is toegestaan
- Pine bepaalt niet of blockers inhoudelijk doorslaggevend zijn
- Pine bepaalt niet de definitieve side
- Pine bepaalt niet de definitieve stop
- Pine bepaalt niet de definitieve target
- Pine bepaalt niet de definitieve R
- Pine bepaalt niet de definitieve execution permission

Pine mag alleen:
- ruwe observaties sturen
- compacte contextuele hints sturen
- potentieel interessante situaties doorgeven

Alle DET-semantiek moet bot-side ontstaan.

---

## 4. ARCHITECTURALE HOOFDLAGEN

De totale beslisketen bestaat uit zes strikt gescheiden lagen.

### LAAG 1 — OBSERVATION LAYER
Feitelijke input uit marktdata en Pine payload.

Voorbeelden:
- symbol
- timeframe
- session_name
- minutes_from_open
- price
- vwap_price
- vwap_slope_1m
- vwap_slope_5m
- bar ranges
- wick ratios
- sweep_detected
- rejection_detected
- structure anchors

Eigenschap:  
geen conclusie, alleen observatie.

### LAAG 2 — TRUTH ENGINE
Bot-side betekenisgeving van observaties.

Voorbeelden:
- regime bruikbaar of rommelig
- prijs te ver van fair value of niet
- 5m clean of mixed
- 1m trigger sterk of zwak
- move vroeg, acceptabel of laat
- invalidatie logisch of onlogisch
- stopmodel logisch of onlogisch
- targetmodel logisch of onlogisch
- R efficiënt of zwak
- samenhang sterk, twijfelachtig of onvoldoende

Eigenschap:  
hier ontstaat de echte DET-logica.

### LAAG 3 — CLASSIFICATION LAYER
Inhoudelijk eindoordeel van de kandidaat.

Toegestane uitkomsten:
- REJECT
- SHADOW
- A
- A+

Eigenschap:  
deze laag zegt iets over inhoudelijke tradekwaliteit,  
maar nog niets over technische uitvoerbaarheid.

### LAAG 4 — RISK ENGINE
Aparte laag voor risicoallocatie.

Voorbeelden:
- A-risk regime
- A+-risk regime
- sizing
- daily/session risk budget
- post-loss remming
- regime-based throttling

Eigenschap:  
risico volgt classificatie,  
niet andersom.

### LAAG 5 — EXECUTION SAFETY GATE
Technische en operationele toestemming om een reeds inhoudelijk beoordeelde en risicomatig gealloceerde trade echt uit te voeren.

Voorbeelden:
- geen actieve tradeconflicten
- bracket order technisch mogelijk
- stop vooraf vast
- contract qualification ok
- market state ok
- cooldown ok
- daily/session limits ok
- bot health ok
- queue/execution state consistent

Eigenschap:  
execution_permission is een aparte poort, los van inhoudelijke waarheid.

### LAAG 6 — ORDER ENGINE
Vertaling van toegestaan tradeplan naar broker-uitvoerbare orderwaarden.

Voorbeelden:
- spread-adjusted entry
- tick-rounded stop
- tick-rounded target
- parent / TP / SL order type
- bracket consistency
- market-specific order translation

Eigenschap:  
broker-uitvoerbare prijzen en ordervorm horen downstream,  
niet in de Truth Engine.

---

## 5. WAARHEIDSHIËRARCHIE

Binnen DET is niet alles even zwaar.  
De bot moet werken met een vaste waarheidshiërarchie.

### NIVEAU 1 — HARD FACTS
Objectieve, niet-discussieerbare marktfeiten.

Voorbeelden:
- prijs
- high / low / open / close
- VWAP
- wick ratio
- candle range
- minutes_from_open
- spread / tick context als observatie
- session timing

### NIVEAU 2 — STRUCTURAL FACTS
Objectief afleidbare marktstructuur.

Voorbeelden:
- higher low / lower high
- sweep boven anchor high
- rejection vanaf level
- 5m boven / onder VWAP
- slope positief / negatief / vlak
- expansie vs overlap
- trigger bar respecteert anchor of breekt die juist

### NIVEAU 3 — CONTEXT TRUTH
Bot-side inhoudelijke contextbeoordeling.

Voorbeelden:
- clean trend
- mixed regime
- chop
- late move
- too extended from fair value
- directionele alignment
- onvoldoende directional clarity

### NIVEAU 4 — SETUP / TRIGGER TRUTH
Bot-side inhoudelijke beoordeling van samenhang en entrywaarde.

Voorbeelden:
- setup bestaat wel / niet
- setup is weak / valid / strong
- trigger is weak / valid / strong
- invalidatie is logisch / onlogisch
- follow-through kans is zwak / redelijk / sterk

### NIVEAU 5 — TRADE PLAN TRUTH
Bot-side inhoudelijke beoordeling van:
- side
- entrymodel
- invalidatiemodel
- stopmodel
- targetmodel
- R-kwaliteit

Belangrijke regel:  
dit is nog steeds inhoudelijke logica,  
niet de broker-uitvoerbare ordervertaling.

### NIVEAU 6 — CLASSIFICATION TRUTH
Definitieve DET-conclusie:
- REJECT
- SHADOW
- A
- A+

Belangrijke regel:  
classification truth mag alleen uit bot-side interpretatie ontstaan.

---

## 6. DET-UITKOMSTEN

### REJECT
Definitie:  
inhoudelijk onvoldoende waarheid.

Voorbeelden:
- VWAP-context tegenstrijdig
- 5m te choppy
- 1m trigger rommelig
- sweep zonder follow-through
- setup te ver extended
- timing te laat
- stop of target onlogisch
- R-profiel inhoudelijk zwak
- meerdere kernpijlers spreken elkaar tegen

Actie:
- geen trade
- geen risk allocation
- geen execution
- geen escalatie naar A/A+

### SHADOW
Definitie:  
interessant, maar nog niet goed genoeg voor execution.

Voorbeelden:
- context deels goed, maar niet volledig bevestigd
- trigger aanwezig, maar te zwak
- setup logisch, maar nog niet hard genoeg
- trade inhoudelijk twijfelachtig
- niet volledig aligned
- meerdere elementen zijn bruikbaar, maar één of meer kernpijlers blijven onzeker

Actie:
- niet executen
- geen risk allocation
- wel behouden als betekenisvolle interne uitkomst

### A
Definitie:  
goede, valide, verdedigbare trade.

Kenmerken:
- context klopt
- VWAP-logica klopt
- 5m ondersteunt
- 1m trigger is valide
- invalidatie is logisch
- stopmodel is logisch
- targetmodel is logisch
- R is acceptabel
- timing is niet te laat
- resterende twijfel is beperkt en niet doorslaggevend

Actie:
- mag naar risk engine
- mag daarna naar execution safety gate

### A+
Definitie:  
uitzonderlijk sterke DET-trade.

Kenmerken:
- zeer clean context
- sterke alignment
- duidelijke structurele logica
- sterke trigger
- logische invalidatie
- logische en compacte stop
- passend en sterk targetmodel
- sterke R-kwaliteit
- minimale inhoudelijke twijfel
- geen inhoudelijke conflicten tussen de kernpijlers

Actie:
- mag naar risk engine
- mag daarna naar execution safety gate met A+-risicoregime

---

## 7. VASTE BEOORDELINGSKETEN

Elke kandidaat moet altijd in exact dezelfde volgorde worden beoordeeld.

### TRUTH CHAIN

#### STAP 1 — REGIME / SESSION TRUTH
Vraag:  
is dit überhaupt een bruikbare marktomgeving voor DET?

Beoordeel:
- sessie
- minuten sinds open
- open-drive of late fase
- volatility regime
- instrument gedrag
- spread / executability context als observatie, niet als brokerbeslissing

Output:
- invalid
- usable
- strong

#### STAP 2 — FAIR VALUE / VWAP TRUTH
Vraag:  
hoe verhoudt prijs zich tot fair value?

Beoordeel:
- boven / onder VWAP
- slope 1m
- slope 5m
- vlak vs directional
- afstand tot VWAP
- retest / pullback / extension

Output:
- weak
- aligned
- strong

#### STAP 3 — 5M CONTEXT TRUTH
Vraag:  
is de hogere context clean genoeg?

Beoordeel:
- trend vs chop
- body dominance
- wick behavior
- overlap
- compressie vs expansie
- sweep / rejection
- maturity van de move

Output:
- dirty
- mixed
- clean
- strong

#### STAP 4 — 1M STRUCTURE TRUTH
Vraag:  
heeft de microstructuur echte executionwaarde?

Beoordeel:
- HL / LH structuur
- re-acceleration
- trigger bar kwaliteit
- wick kwaliteit
- pullback depth
- invalidatie-logica

Output:
- weak
- usable
- clean
- strong

#### STAP 5 — SETUP TRUTH
Vraag:  
bestaat er een echte DET-setup als samenhangend geheel?

Beoordeel:
- regime + VWAP + 5m + 1m + timing + structurele samenhang

Output:
- false
- weak
- valid
- strong

#### STAP 6 — TRIGGER TRUTH
Vraag:  
is er nu een echte trade-trigger of alleen potentie?

Beoordeel:
- timing
- invalidatie
- entry-logica
- follow-through kans
- ruis vs scherpte

Output:
- false
- weak
- valid
- strong

#### STAP 7 — TIMING TRUTH
Vraag:  
komt deze trade nog op tijd of is de move inhoudelijk al te laat?

Beoordeel:
- early
- acceptable
- late

#### STAP 8 — TRADE PLAN TRUTH
Vraag:  
laat deze setup een inhoudelijk logisch tradeplan toe?

Beoordeel:
- derived side
- entrymodel
- invalidatiemodel
- stopmodel
- targetmodel
- samenhang tussen stop, target en context

Output:
- false
- weak
- valid
- strong

#### STAP 9 — R TRUTH
Vraag:  
laat de setup een logisch en efficiënt risk/reward-profiel toe?

Beoordeel:
- poor
- acceptable
- efficient

Belangrijke regel:  
R-truth wordt beoordeeld binnen de context van:
- instrument
- setup family
- timing
- gekozen stopmodel
- gekozen targetmodel

R is dus niet universeel markt-onafhankelijk.

#### STAP 10 — CLASSIFICATION TRUTH
Output:
- REJECT
- SHADOW
- A
- A+

### EXECUTION CHAIN

#### STAP 11 — RISK ENGINE
Vraag:  
welk risicoregime hoort bij deze inhoudelijke classificatie?

Output:
- no allocation
- A-risk regime
- A+-risk regime

#### STAP 12 — EXECUTION SAFETY GATE
Vraag:  
mag deze inhoudelijk beoordeelde trade technisch en operationeel echt uitgevoerd worden?

Output:
- execute = yes
- execute = no
- blockers = [...]

Belangrijke architectuurregel:  
stap 1 t/m 10 zijn truth / classification.  
stap 11 en 12 zijn downstream beslislagen.  
Deze mogen nooit teruglekken en de inhoudelijke waarheid herschrijven.

---

## 8. INHOUDELIJKE DET-PIJLERS

De Truth Engine moet elke kandidaat impliciet langs zeven inhoudelijke pijlers leggen.

### PIJLER 1 — CONTEXTKWALITEIT
Is de markt schoon genoeg om DET-logica betrouwbaar toe te passen?

### PIJLER 2 — RICHTINGSLOGICA
Is de gekozen richting logisch ten opzichte van VWAP, structuur en flow?

### PIJLER 3 — STRUCTUURVALIDITEIT
Is er echte structurele onderbouwing of slechts toevalsbeweging?

### PIJLER 4 — TRIGGERKWALITEIT
Is er een echte, verdedigbare entry of wordt achter prijs aangejaagd?

### PIJLER 5 — TIMINGKWALITEIT
Is de setup nog op tijd of is de move inhoudelijk te laat?

### PIJLER 6 — TRADEPLAN-KWALITEIT
Is de combinatie van invalidatie, stopmodel en targetmodel inhoudelijk logisch?

### PIJLER 7 — R-KWALITEIT
Laat de setup een logisch en efficiënt risk/reward-profiel toe binnen deze markt en setup?

Belangrijke noot:  
executionveiligheid is geen inhoudelijke pijler van de Truth Engine,  
maar een aparte downstream gate.

---

## 9. HARDE INHOUDELIJKE REGELS

### RULE 1
Pine levert observaties, nooit beslissingen.

### RULE 2
Bij inhoudelijke twijfel nooit EXECUTE.  
Altijd SHADOW of REJECT.

### RULE 3
Geen trigger zonder context.  
Een losse 1m candle zonder valide context is geen DET-trade.

### RULE 4
Geen trade zonder vooraf bekende invalidatie.  
Dus geen trade zonder logische stoplogica.

### RULE 5
Geen A of A+ als de move inhoudelijk te laat is.

### RULE 6
R-kwaliteit is onderdeel van truth.  
Een “mooie” setup met slecht R-profiel is geen goede trade.

### RULE 7
Classificatie, risicoallocatie en execution_permission zijn strikt gescheiden.

### RULE 8
SHADOW is een volwaardige beschermingsuitkomst, geen mislukte A.

### RULE 9
Pine-semantiek mag nooit execution authority worden.

### RULE 10
Bij technische onveiligheid nooit forceren.  
Ook A / A+ wordt dan niet geëxecuteerd.

### RULE 11
Bij conflicterende kernwaarheden mag de engine nooit escaleren naar A+.

### RULE 12
Bij meerdere inhoudelijke conflicten moet de classificatie systematisch degraderen naar SHADOW of REJECT.

### RULE 13
SL-, TP- en R-logica mogen per markt verschillen.

### RULE 14
SL-, TP- en R-logica mogen ook per setup-family verschillen.

### RULE 15
De Truth Engine bepaalt de inhoudelijke logica van stopmodel, targetmodel en R-kwaliteit,  
maar niet de definitieve broker-uitvoerbare prijsvertaling.

---

## 10. DEGRADATIELOGICA

De Truth Engine moet niet alleen classificeren, maar ook systematisch degraderen.

### A+ vereist minimaal:
- regime bruikbaar tot sterk
- VWAP-context aligned of strong
- 5m context clean of strong
- 1m structure clean of strong
- setup_truth strong of minstens overtuigend valid
- trigger_truth strong
- timing_truth acceptable of early
- trade_plan_truth valid of strong
- r_truth efficient
- geen harde inhoudelijke conflicten
- minimale twijfel

### A vereist minimaal:
- regime usable of better
- VWAP-context niet-conflicterend
- 5m context minstens mixed maar inhoudelijk bruikbaar
- 1m structure minstens usable
- setup_truth valid
- trigger_truth valid
- timing_truth niet late
- trade_plan_truth valid
- r_truth acceptable of better
- beperkte twijfel toegestaan

### SHADOW ontstaat wanneer:
- de trade inhoudelijk interessant is
- maar één of meer kernpijlers onvoldoende overtuigend zijn
- of de samenhang nog te zwak is voor A
- of één duidelijke zwakte degradatie noodzakelijk maakt zonder directe totale afwijzing

### REJECT ontstaat wanneer:
- hard blockers aanwezig zijn
- meerdere kernpijlers elkaar tegenspreken
- invalidatie onlogisch is
- stopmodel onlogisch is
- targetmodel onlogisch is
- timing inhoudelijk te laat is
- R-kwaliteit poor is
- context, setup of trigger fundamenteel onvoldoende waarheid bevat

Belangrijke regel:  
degradatie gaat altijd in veilige richting.

A+ → A  
A → SHADOW  
SHADOW → REJECT

Nooit andersom zonder nieuwe inhoudelijke waarheid.

---

## 11. INPUTMODEL

De Truth Engine accepteert alleen compacte, relevante input.

### HARD REQUIRED OBSERVATIONS
- symbol
- timeframe
- bar_time_unix_ms
- session_name
- minutes_from_open
- price
- vwap_price
- vwap_slope_1m
- vwap_slope_5m
- trigger_bar_high
- trigger_bar_low
- structure_anchor_low
- structure_anchor_high
- sweep_detected
- sweep_side
- rejection_detected

### REQUIRED ENRICHED OBSERVATIONS
- distance_from_vwap_atr
- ema_spread_atr_value
- htf_ema_spread_atr
- bar_range_1m
- bar_body_1m
- upper_wick_ratio_1m
- lower_wick_ratio_1m
- pullback_depth_1m
- trigger_close_location
- trigger_range_expansion
- bar_range_5m
- bar_body_5m
- upper_wick_ratio_5m
- lower_wick_ratio_5m

### OPTIONAL OBSERVATION LAYER
- above_vwap
- candidate_side
- body_strength_value
- rejection_wick_ratio
- trend_strength_5m
- reacceleration_detected
- htf_trend_up
- htf_trend_down
- htf_vwap_up
- htf_vwap_down
- htf_vwap_not_flat

### VERBODEN ALS PINE-AUTHORITY
Onder andere:
- session_valid
- setup_valid
- trigger_valid
- execution_candidate_valid
- blocker
- blocker_count
- reason_flags
- score
- grade
- candidate_grade
- tv_candidate_grade

Deze mogen hoogstens als legacy debug-info bestaan,  
maar nooit als beslissingsautoriteit.

Aanvullende projectregel:  
velden die een verkapte interpretatie of classificatie bevatten,  
mogen nooit als required core truth-input worden behandeld.

---

## 12. MARKTPROFIELEN EN SETUP-PROFIELEN

De Truth Engine blijft één centrale engine,  
maar moet kunnen werken met meerdere marktprofielen en setup-profielen.

### BELANGRIJKE REGEL
Er is niet voor elke markt een compleet ander DET-systeem.  
Er is één Truth Engine met uniforme logica,  
maar met markt-specifieke en setup-specifieke parametrisering.

### MARKTPROFIELEN KUNNEN ONDER MEER INVLOED HEBBEN OP:
- wat als compacte of ruime stop geldt
- wat als acceptabele extension geldt
- wat als acceptabele R geldt
- welk targetmodel vaker logisch is
- hoeveel noise / wickiness normaal is
- hoeveel buffer structureel nodig is
- hoe streng timing geïnterpreteerd moet worden

### SETUP-PROFIELEN KUNNEN ONDER MEER INVLOED HEBBEN OP:
- type entrymodel
- type invalidatie
- type stopmodel
- type targetmodel
- minimale verwachte follow-through
- minimale R-drempel voor A of A+

### VOORBEELDREGEL
MES, MNQ, M6E en FDXM mogen dus verschillende inhoudelijke stop- en targetlogica kennen,  
zonder dat de centrale DET-waarheidslogica wordt verlaten.

---

## 13. INTERNE OUTPUTSTRUCTUUR

De Truth Engine moet idealiter niet slechts één label teruggeven,  
maar een volledige interne beoordelingsstructuur.

Aanbevolen structuur:

```yaml
truth_assessment:
  market_profile: MES | MNQ | M6E | FDXM | ...
  setup_profile: default | sweep_rejection | continuation | mean_reversion | ...
  regime_truth: invalid | usable | strong
  vwap_truth: weak | aligned | strong
  context_5m_truth: dirty | mixed | clean | strong
  structure_1m_truth: weak | usable | clean | strong
  setup_truth: false | weak | valid | strong
  trigger_truth: false | weak | valid | strong
  timing_truth: early | acceptable | late
  trade_plan_truth: false | weak | valid | strong
  r_truth: poor | acceptable | efficient
  overall_classification: REJECT | SHADOW | A | A+
  derived_side: long | short | none
  derived_entry_model: none | pullback_limit | breakout_limit | confirmation_entry
  derived_invalidation_model: none | structure_break | trigger_failure | anchor_loss | hybrid
  derived_stop_model: none | structural | trigger_bar | anchor_based | volatility_band | hybrid
  derived_target_model: none | fixed_R | structure_based | liquidity_pool | fair_value_reversion | session_expansion | hybrid
```

Belangrijke noot:  
execution_permission hoort niet in de truth_assessment zelf.

Downstream outputstructuur:

```yaml
risk_assessment:
  risk_regime: none | A | A+
  risk_percent: 0 | ...

execution_assessment:
  execution_permission: true | false
  execution_blockers: [...]
  execution_route: none | bracket_only
```

Doel:  
volledige debugbaarheid,  
transparantie,  
strikte interne logica,  
en heldere laagafbakening.

---

## 14. KOPPELING MET RISK ENGINE

De Truth Engine bepaalt niet zelfstandig het risico.  
De Truth Engine levert alleen de inhoudelijke classificatie aan.

Koppeling:
- REJECT → geen risk allocation
- SHADOW → geen risk allocation
- A → A-risk regime
- A+ → A+-risk regime

Belangrijk:  
risk allocation gebeurt pas na classificatie  
en blijft een aparte laag.

Dus:

**Truth Engine**  
→ “wat is dit inhoudelijk?”

**Risk Engine**  
→ “hoeveel risico hoort hierbij?”

**Execution Safety Gate**  
→ “mag ik dit nu operationeel uitvoeren?”

Belangrijke nuance:  
hetzelfde classificatielabel kan per markt nog steeds tot een andere praktische sizing en ordervertaling leiden,  
omdat tick value, volatiliteit, spread en normale stopafstand verschillen.

---

## 15. KOPPELING MET ORDERLOGICA

De Truth Engine moet input leveren aan de orderlaag,  
maar mag niet verward worden met orderuitvoering.

De Truth Engine moet bot-side afleiden:
- side
- entry model
- invalidation model
- stop model
- target model
- R-basis
- inhoudelijke haalbaarheid van het tradeplan

De orderlaag vertaalt dit vervolgens naar:
- executable entry price
- spread-adjusted price
- tick-rounded stop
- tick-rounded target
- parent order
- take profit
- stop loss
- bracket consistency
- IBKR contract / order plaatsing

Belangrijke regel:  
geen broker-specifieke rommel in de Truth Engine.  
Alle brokeruitvoering blijft downstream.

Extra architectuurregel:  
inhoudelijk model en executable prijsniveau zijn niet hetzelfde.  
De Truth Engine bepaalt logica.  
De orderlaag bepaalt broker-uitvoerbare waarden.

Aanvullende regel:  
stopmodel, targetmodel en R-logica zijn niet universeel gelijk over markten heen.  
De orderlaag moet per markt correct vertalen wat de Truth Engine inhoudelijk heeft afgeleid.

---

## 16. EXECUTION SAFETY GATE

Zelfs een A of A+ mag alleen uitgevoerd worden als alle executionvoorwaarden groen zijn.

Minimaal:
- geen actieve trade
- geen operationeel onvolledige vorige trade
- contract qualification ok
- market state ok
- order type ondersteund
- bracket order verplicht
- stop vooraf vast
- position sizing valide
- cooldown regels ok
- daily / session risk limit niet geraakt
- bot health ok
- queue / execution state consistent

Als één van deze faalt:  
`execution_permission = false`

Belangrijke regel:  
een execution denial verandert de inhoudelijke classificatie niet automatisch.  
Een A blijft inhoudelijk A, ook als execution technisch niet is toegestaan.

---

## 17. ANTI-PATTERNS

De volgende anti-patterns zijn expliciet verboden.

### ANTI-PATTERN 1
Pine-labels behandelen als waarheid.

### ANTI-PATTERN 2
Classificatie en execution samenvoegen.

### ANTI-PATTERN 3
Brokerbeperkingen laten teruglekken in DET-logica.

### ANTI-PATTERN 4
Een losse trigger candle behandelen als complete setup.

### ANTI-PATTERN 5
Te veel Pine-velden toevoegen die slechts verborgen beslissingen zijn.

### ANTI-PATTERN 6
SHADOW overslaan om toch meer trades te forceren.

### ANTI-PATTERN 7
R-kwaliteit negeren bij setupbeoordeling.

### ANTI-PATTERN 8
Bij twijfel alsnog A geven “omdat de alert er goed uitziet”.

### ANTI-PATTERN 9
Execution denial terugvertalen naar inhoudelijke REJECT zonder aparte reden.

### ANTI-PATTERN 10
Executable broker-prijzen behandelen alsof ze hetzelfde zijn als truth-modellen.

### ANTI-PATTERN 11
Voor alle markten één vaste universele SL / TP / R-formule afdwingen.

### ANTI-PATTERN 12
Pine verborgen target- of stopsemantiek laten bepalen wat inhoudelijk “goed” is.

---

## 18. ARCHITECTURALE PLAATS IN DE BOT

Aanbevolen logische flow:

1. Webhook ontvangt payload
2. Payload technische validatie
3. Kandidaat naar execution queue
4. Worker pakt kandidaat
5. Symbol / contract / instrument context ophalen
6. Observation layer opbouwen
7. DET Truth Engine uitvoeren
8. truth_assessment opbouwen
9. overall classification bepalen
10. Risk Engine toepassen indien A / A+
11. Execution Safety Gate draaien
12. Alleen bij toestemming: orderplan opbouwen
13. Alleen bij toestemming: market-specific executable prijzen afleiden
14. Alleen bij toestemming: bracket genereren en plaatsen
15. Trade state + logs + observability bijwerken

Belangrijk:  
de Truth Engine leeft in de bot core,  
niet in de webhook,  
niet in Pine,  
niet in de brokeradapter.

---

## 19. MINIMALE SUCCESCRITERIA

De DET Truth Engine is pas architectonisch geslaagd als:
- Pine geen inhoudelijke authority meer is
- REJECT / SHADOW / A / A+ volledig bot-side ontstaan
- classificatie, risico en execution strikt gescheiden zijn
- de bot per kandidaat uitlegbaar kan maken waarom iets rejected, shadow, A of A+ is
- stop-logica, targetlogica en R-logica onderdeel zijn van de inhoudelijke beoordeling
- de engine compact blijft en niet verdrinkt in Pine-semantiek
- technische execution blockers apart zichtbaar zijn
- twijfel structureel naar SHADOW / REJECT leidt
- executable orderwaarden downstream blijven en niet de truth-laag vervuilen
- markt-specifieke SL / TP / R-logica mogelijk is zonder de centrale Truth Engine te breken

---

## 20. KORTE KERNFORMULE

Observations  
→ Context Truth  
→ Setup Truth  
→ Trigger Truth  
→ Trade Plan Truth  
→ Classification Truth  
→ Risk Allocation  
→ Execution Safety  
→ Order Execution

Of nog compacter:

**See clearly.  
Interpret locally.  
Classify strictly.  
Allocate separately.  
Execute safely.**

---

## 21. DEFINITIEVE PROJECTREGEL

Binnen het IKBR Scalping Bot project geldt voortaan:

De DET Truth Engine is de enige inhoudelijke authority voor tradekwaliteit.  
Pine mag alleen observaties en beperkte hints leveren.  
Alle DET-interpretatie en classificatie ontstaan bot-side.  
Risicoallocatie ontstaat pas na classificatie in de Risk Engine.  
Execution-toestemming ontstaat pas daarna in de Execution Safety Gate.  
Executable orderwaarden en brokeruitvoering ontstaan pas downstream in de order- en brokerlaag.

Daarbij geldt expliciet:  
SL-, TP- en R-logica mogen per markt en per setup-family verschillen,  
maar die verschillen moeten bot-side, expliciet, uitlegbaar en architectonisch beheerst ontstaan,  
zonder Pine-authority en zonder de centrale DET-waarheidslogica te verlaten.

Dat is de vaste architectuurlijn voor alle volgende analyses, prompts, patches en codebeslissingen.
