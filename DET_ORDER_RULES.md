ROLE = ARCHITECT

OVERDRACHTSDOCUMENT v3 — DET ORDER RULES VOOR DE SCALPINGBOT

DOEL
Dit document vertaalt de DET-orderregels naar duidelijke architectuurregels voor de scalpingbot.

Dit document gaat NIET over technische runtime-stabiliteit,
maar over inhoudelijke order-, selectie- en risicoregels volgens het DET-model.

Uitgangspunt:
de bot mag niet worden gebouwd als een emotieloze versie van een discretionaire trader,
maar als een systeem dat DET-logica strikt, voorspelbaar en herhaalbaar uitvoert.

De centrale vraag is:
welke orderregels horen inhoudelijk bij DET,
en hoe moeten die worden vertaald naar een automatische scalpingbot
zonder psychologische mensentaal te verwarren met machine-logica?

1. HOOFDCONCLUSIE

De DET-orderregels moeten in de bot worden vertaald als drie aparte lagen:

1. selectiediscipline
2. executiondiscipline
3. risicoregiemdiscipline

Belangrijkste hoofdconclusie:

De menselijke regel
“stoppen na SL om revenge trades te voorkomen”
blijft inhoudelijk deels zinvol voor de bot,
maar NIET om psychologische redenen.

Voor de bot moet deze regel worden hergeformuleerd als:

NA EEN SL MAG DE BOT NIET DIRECT OPNIEUW EXECUTEN
IN DEZELFDE OF INHOUDELIJK GELIJKWAARDIGE SLECHTE CONTEXT,
TENZIJ ER EEN DUIDELIJKE CONTEXT- OF REGIME-RESET IS.

Dus:
- bij een mens voorkomt deze regel emotionele revenge
- bij een bot voorkomt deze regel regime-herhaling, verliesclustering en overtrading in slechte context

2. HOOFDPRINCIPE: PRE-TRADE SELECTIE IS BELANGRIJKER DAN POST-LOSS REMMING

Dit principe moet expliciet boven de rest hangen:

DE BELANGRIJKSTE VERDEDIGINGSLAAG IN DET IS STERKE PRE-TRADE SELECTIE,
NIET PAS REMMEN NA VERLIES.

Dat betekent:
- de bot mag niet ruimer gaan traden omdat er later toch een cooldown of daily stop bestaat
- cooldown en loss caps zijn secundaire bescherming
- primaire bescherming moet komen uit:
  - contextkwaliteit
  - setupkwaliteit
  - triggerkwaliteit
  - strikte execution gating

Volgorde:
1. goede selectie
2. goede executiondiscipline
3. pas daarna verliesbeperking en regime-remming

3. HARD DET ORDER RULES

Deze regels horen hard in de inhoudelijke architectuur thuis.

3.1 Maximaal 1 trade tegelijk
De bot mag maximaal 1 actieve trade tegelijk hebben.

Belangrijk:
dit betekent niet alleen:
- maximaal 1 open positie

Maar ook:
- geen nieuwe execution zolang een eerdere trade administratief, state-machine technisch of operationeel nog niet volledig afgerond is

Dus:
- geen nieuwe trade bij actieve positie
- geen nieuwe trade bij ENTRY_WORKING
- geen nieuwe trade bij EXIT_WORKING
- geen nieuwe trade zolang trade-administratie nog niet volledig in gesloten/eindstaat is

Dit moet inhoudelijk gelezen worden als:

MAXIMAAL 1 ACTIEVE OF OPERATIONEEL ONVOLLEDIGE TRADE TEGELIJK.

3.2 Alleen bracket orders
Elke trade moet vooraf volledig gedefinieerd zijn met:
- entry
- stop-loss
- take-profit

Waarom:
- DET vereist vooraf bekende invalidatie
- DET vereist vooraf bekende R-structuur
- voorkomt discretionaire improvisatie

Architectuurvertaling:
- geen losse entries
- geen stop later toevoegen
- geen open eindes

3.3 Stop-loss moet vooraf vaststaan
Een DET-trade zonder duidelijke invalidatie is geen valide trade.

Architectuurvertaling:
- stop is verplicht onderdeel van de tradeplanning
- geen ordervrijgave zonder stop

3.4 Vast R-model
Het DET-model hoort een vast en voorspelbaar risicomodel te hebben.

In projectcontext:
- A = lager risico
- A+ = hoger risico
- target gebaseerd op 2R-model

Architectuurvertaling:
- execution_grade bepaalt risicoprofiel
- risico en target mogen niet opportunistisch verschuiven

3.5 Geen trade zonder context + setup + trigger
Elke executeerbare trade vereist:
- valide context
- valide setup
- valide trigger

Zonder volledige keten is er geen trade.

Belangrijk:
- mooie candle alleen is niet genoeg
- sweep alleen is niet genoeg
- VWAP-bias alleen is niet genoeg
- context zonder trigger is geen execution

3.6 Geen averaging down / averaging up in verlies
DET-scalping is geen rescue-, grid- of martingale-model.

Architectuurvertaling:
- geen toevoegen aan verliezende positie
- geen herstelorders
- invalidatie = trade-these gebroken

3.7 Inhoudelijke twijfel betekent nooit automatische execution
Dit moet expliciet als harde regel worden vastgelegd:

BIJ INHOUDELIJKE TWIJFEL WORDT EEN KANDIDAAT GEDEGRADEERD NAAR SHADOW OF REJECT,
NOOIT NAAR EXECUTE.

Operationele vertaling:
- duidelijke negatieve inhoudelijke breuk of harde blocker = REJECT
- inhoudelijke twijfel, borderline kwaliteit of onvoldoende overtuiging = SHADOW
- pas duidelijke, voldoende overtuigende kwaliteit = EXECUTE_A of EXECUTE_A_PLUS

Dus:
- borderline is geen A
- twijfel is geen A+
- inhoudelijke onzekerheid mag nooit via een achterdeur execution krijgen

4. STERK AAN TE RADEN DET ORDER RULES

Deze regels zijn geen absolute basiswetten,
maar inhoudelijk zeer sterk aanbevolen.

4.1 Maximaal 1 execution attempt per unieke setup-context
Na een mislukte trade mag de bot niet direct opnieuw dezelfde context behandelen als een nieuwe execute-kans,
zonder duidelijke reset.

Waarom:
- voorkomt machineversie van pseudo-revenge
- voorkomt herhaald schieten op vrijwel dezelfde foutieve setup
- bewaakt DET-selectiviteit

Architectuurvertaling:
- context / setup fingerprinting of inhoudelijke contextvergelijking
- geen her-entry zonder reset

4.2 Cooldown na stop-loss
Na een SL moet een remmechanisme actief worden.

Belangrijk:
dit hoeft NIET automatisch te betekenen:
- hele dag stoppen

Wel betekent het:
- geen directe her-entry
- eerst afkoeling of reset
- eerst nieuwe contextbeoordeling

Architectuurvertaling:
- tijdscooldown
- of 5m context reset
- of regime reset
- of combinatie

4.3 Maximaal aantal verliestrades per dag of sessie
Voorbeelden:
- max 1 verliestrade per instrument per sessie
- of max 2 verliestrades per dag
- of daily loss cap

Belangrijk:
dit is een conservatieve implementatieregel,
geen eeuwige DET-wet.

Dus:
- dit is vooral geschikt voor eerste automatische live fase
- later kan dit verfijnd worden door slimmere context-resetlogica

4.4 Geen nieuwe trade in dezelfde chop-/late-/mislukte context
Als de laatste trade verloor in een inhoudelijk slecht regime,
dan moet de bot extra streng zijn voordat een volgende execution wordt toegestaan.

Voorbeelden:
- chop-regime
- late extension
- conflicterende context
- mislukte continuation zonder reset

5. BELANGRIJK ARCHITECTUURPRINCIPE: SETUPFAMILIE-BLOKKERING

Dit moet prominenter in de architectuur komen:

DE BOT MAG NA EEN VERLIESTRADE NIET OPNIEUW EXECUTEN OP EEN INHOUDELIJK GELIJKWAARDIGE SETUPFAMILIE
ZONDER DUIDELIJKE RESET.

Waarom:
een menselijke trader kan vaak aanvoelen:
“dit is eigenlijk gewoon dezelfde trade nog een keer.”

De bot moet dat systematisch modelleren.

Voorbeelden van inhoudelijk equivalente setupfamilie:
- zelfde richting
- zelfde VWAP-relatie
- zelfde HTF-context
- zelfde type continuation/reclaim-idee
- zelfde mislukte structuurzone
- zelfde sessiefase zonder nieuwe reset

Belangrijk:
dit is nu nog een inhoudelijk architectuurprincipe,
geen volledig uitgewerkte technische specificatie.

Er moet later een aparte mini-specificatie komen voor:
- welke velden equivalentie bepalen
- hoe zwaar elk veld meeweegt
- wanneer iets “dezelfde familie” is
- wanneer iets inhoudelijk echt nieuw is

6. WAT “NIEUWE 5M-CONTEXT” BETEKENT

Dit moet expliciet worden aangescherpt.

Een nieuwe 5m-context betekent NIET automatisch:
- nieuwe candle = reset

Een nieuwe 5m-context betekent:

EEN INHOUDELIJK NIEUWE SETUP-OMGEVING.

Dat kan bijvoorbeeld impliceren:
- nieuwe directional intent
- nieuwe VWAP-relatie
- nieuwe structuurontwikkeling
- nieuwe sweep/reclaim/hold
- duidelijke breuk met de vorige mislukte context

Dus:
een simpele nieuwe bar is onvoldoende.
Er moet een inhoudelijke reset zijn, niet alleen een tijdtechnische reset.

Belangrijk:
dit is in deze fase nog een inhoudelijke richtlijn,
geen volledig toetsbare technische definitie.

Er moet later een aparte specificatie komen voor:
- minimale resetcriteria
- sterke resetcriteria
- en welke signalen onvoldoende zijn om als reset te gelden

7. DET-SPECIFIEKE KWALITEITSREGELS

7.1 Geen entry als move inhoudelijk te laat is
Late extensie blijft een zwaar of hard afwijscriterium.

7.2 Geen entry in chop / random marktkwaliteit
Compact betekent niet automatisch chop,
maar echte random directional chaos moet blokkeren.

7.3 A+ alleen bij uitzonderlijk schone alignment
A+ moet zeldzaam en betekenisvol blijven.

7.4 SHADOW is nooit execute
SHADOW blijft leer-/observatieklasse.
Niet execution-klasse.

7.5 Pine is nooit execution authority
Pine mag detecteren.
De bot beslist inhoudelijk.

8. SPECIFIEKE VRAAG: IS “STOPPEN NA SL” NOG ZINVOL VOOR EEN BOT?

Ja, maar de reden verandert.

Bij een mens:
- stoppen na SL voorkomt revenge trades

Bij een bot:
- stoppen of afremmen na SL voorkomt herhaald handelen in dezelfde slechte context
- begrenst drawdown
- voorkomt verliesclustering
- dwingt contextreset af

Dus:
de regel blijft inhoudelijk zinvol,
maar NIET om psychologische redenen.

De juiste botvertaling is:

NA EEN SL MAG DE BOT NIET DIRECT OPNIEUW EXECUTEN
TENZIJ ER EEN NIEUWE, INHOUDELIJK GELDIGE CONTEXT IS ONTSTAAN.

9. DRIE BOT-VERTALINGEN VAN DEZE REGEL

9.1 Conservatieve eerste live fase
- max 1 trade tegelijk
- max 1 verliestrade per instrument per sessie
- daarna stoppen voor die sessie of dat instrument

Belangrijk:
dit is een veilige implementatieregel,
geen DET-dogma voor altijd.

9.2 Middelvariant
- na SL geen directe re-entry
- verplichte cooldown
- nieuwe 5m-context vereist
- vorige setup moet inhoudelijk voorbij zijn

9.3 Volwassen bot-regel
- max daily loss
- max consecutive losses
- cooldown na SL
- regime reset vereist
- setup-family blocking
- context fingerprint / equivalence detection

10. BELANGRIJK SCHEIDINGSPRINCIPE: DET CLASSIFICATION ≠ RISK ENGINE ≠ EXECUTION PERMISSION

Dit moet expliciet hard worden vastgelegd.

DET-classificatie, risk engine en execution-permission zijn drie aparte lagen.

Voorbeeld:
- een trade kan inhoudelijk A zijn
- maar execution_permission kan false zijn wegens daily loss cap
- of wegens cooldown
- of wegens session stop rule

Dus niet:
- “geen A meer want daily loss bijna bereikt”

Wel:
- “inhoudelijk A, maar operationeel niet toegestaan”

Scheiding:
- DET-classificatie = inhoudelijk oordeel
- risk engine = risico-/regimeregels
- execution_permission = finale operationele vrijgave

11. VEILIGHEIDSPROFIELEN PER FASE

FASE 1 — classification-safe
Doel:
- inhoudelijke DET-beoordeling stabiel krijgen
- geen automatische execution op borderline interpretaties

Regels:
- 1 trade tegelijk
- bracket orders verplicht
- stop vooraf vast
- vast A / A+ risicomodel
- geen averaging
- SHADOW nooit executen
- Pine nooit execution authority
- inhoudelijke twijfel = geen execute

FASE 2 — controlled execution-safe
Doel:
- eerste voorzichtige automatische DET-execution onder strakke veiligheidsrails

Regels:
- max 1 verliestrade per instrument per sessie
- cooldown na SL
- nieuwe inhoudelijke 5m-context vereist
- daily loss cap
- expliciete scheiding tussen DET-classificatie en execution_permission

FASE 3 — mature adaptive risk regime
Doel:
- volwassen DET-bot met contextgevoelige herstart- en remregels

Regels:
- regime-sensitive re-entry rules
- max consecutive losses
- setup-family blocking
- context fingerprinting
- inhoudelijke resetlogica
- verfijnde herstartregels na SL

12. PRAKTISCHE EINDCONCLUSIE

De regel “stoppen na SL” moet voor de scalpingbot niet worden verwijderd,
maar inhoudelijk opnieuw worden gedefinieerd.

Niet:
- stoppen om revenge trades te voorkomen

Wel:
- stoppen of afremmen om te voorkomen dat de bot opnieuw handelt in dezelfde mislukte context
- regimeverandering af te wachten
- verliesclustering te beperken
- DET-selectiviteit te bewaken

De belangrijkste architectuurboodschap is:

PRE-TRADE SELECTIE BLIJFT DE EERSTE VERDEDIGINGSLAAG.
POST-LOSS REMMING IS BELANGRIJK, MAAR SECUNDAIR.

KORTE SAMENVATTING

Belangrijkste DET order rules voor de bot:
- maximaal 1 actieve of operationeel onvolledige trade tegelijk
- alleen bracket orders
- stop vooraf vast
- vast A / A+ risicomodel
- geen trade zonder context + setup + trigger
- geen averaging
- SHADOW nooit executen
- Pine nooit execution authority
- inhoudelijke twijfel = SHADOW of REJECT, nooit execute
- cooldown of contextreset na SL
- daily/session loss limits in vroege live fase
- setup-family blocking bij volwassen versie

Belangrijkste nuance:
“stoppen na SL” blijft ook voor een bot zinvol,
maar niet om psychologische revenge te voorkomen.
Voor de bot is de juiste reden:
regimecontrole, kwaliteitsbewaking en drawdownbeperking.