# DET_DECISION_MODEL_FOR_SCALPINGBOT.md

## 1. DOEL EN REIKWIJDTE

### Doel van het DET-model binnen de scalpingbot
Het DET-model binnen de scalpingbot heeft als doel om kandidaat-signalen uit TradingView inhoudelijk te beoordelen voordat zij ooit execution-waardig worden verklaard. De bot-side decision layer is bedoeld als een tweede inhoudelijke beoordelingslaag bovenop detectie. Deze laag moet voorkomen dat elk technisch gedetecteerd TradingView-signaal automatisch als trade-kans wordt behandeld.

De decision layer beoordeelt of een kandidaat-signaal:
- inhoudelijk direct moet worden afgewezen;
- inhoudelijk interessant is maar nog niet execution-waardig is;
- inhoudelijk voldoet aan de DET-voorwaarden voor een A-setup;
- inhoudelijk voldoet aan de DET-voorwaarden voor een A+-setup.

### Wat TradingView beslist
TradingView beslist uitsluitend over detectie en signalering. TradingView mag:
- contextuele en technische kenmerken detecteren;
- enriched payloads genereren met context-, structuur- en triggervelden;
- kandidaat-signalen uitsturen die door de bot inhoudelijk worden herbeoordeeld.

TradingView beslist **niet** definitief of een trade uitgevoerd moet worden.

### Wat de bot beslist
De bot beslist inhoudelijk over classificatie. De bot-side decision layer bepaalt of een kandidaat-signaal wordt geclassificeerd als:
- `REJECT`
- `SHADOW`
- `EXECUTE_A`
- `EXECUTE_A_PLUS`

Deze beslissing is gebaseerd op DET-logica en niet alleen op het bestaan van een TradingView-alert.

### Wat de bot voorlopig nog NIET beslist
De bot beslist voorlopig nog niet autonoom over volledige live execution als eindverantwoordelijke handelsbeslissing. In deze fase is het doel:
- candidate signals inhoudelijk beoordelen;
- classificeren;
- loggen;
- later vergelijken met uitkomsten;
- pas in een latere fase execution-vrijgave conditioneel toestaan.

De bot-side decision layer is in deze fase dus primair een **decision and evaluation layer**, nog geen volledig zelfstandige live trader.

---

## 2. KERNPRINCIPES VAN HET DET-MODEL

### VWAP-centrisch
DET is in de kern VWAP-centrisch. VWAP is de primaire referentie voor:
- directional bias;
- contextuele locatie;
- kwaliteit van reclaim, rejectie en continuatie;
- beoordeling of een move nog vers is of al ver geëxpandeerd is.

Een setup zonder duidelijke relatie tot VWAP is geen DET-setup.

### Belang van sessie en opening
DET is geen willekeurig all-day model. Sessiecontext is essentieel. De hoogste kwaliteit wordt verwacht rond marktmomenten met echte orderflow en directional intent, met name:
- direct bij marktopening;
- kort na marktopening;
- momenten waarop prijs en VWAP-context snel informatie geven over bias en acceptatie.

Setupkwaliteit buiten relevante sessiemomenten moet strenger worden behandeld.

### 5m context
De 5m-laag bepaalt of de markt contextueel bruikbaar is. Deze laag beoordeelt:
- bias;
- VWAP-richting;
- trendmatige bruikbaarheid;
- structuurkwaliteit;
- marktkwaliteit;
- of een omgeving überhaupt DET-waardig is.

Zonder valide 5m-context is er geen DET-setup.

### 1m timing en trigger
De 1m-laag bepaalt timing en trigger. Deze laag beoordeelt:
- entrykwaliteit;
- wick-behavior;
- rejectie;
- reclaim-hold;
- re-acceleratie;
- of de entry nog fris en bruikbaar is.

Een goede 5m-context zonder goede 1m-trigger is geen executeerbare DET-setup.

### Kwaliteit boven frequentie
DET kiest kwaliteit boven aantal trades. Het model is ontworpen om goede situaties te selecteren en veel middelmatige situaties af te wijzen. Het doel is niet maximale alertfrequentie, maar hoge inhoudelijke selectie.

### Geen trade is ook juiste uitkomst
Geen trade is binnen DET een volledig correcte uitkomst. Een kandidaat-signaal mag en moet worden afgewezen als context, structuur of trigger niet voldoende is.

### Onderscheid tussen context, setup en execution
DET maakt strikt onderscheid tussen:
- **context**: is de markt in principe bruikbaar?
- **setup**: is er een inhoudelijk valide A of A+ kandidaat?
- **execution**: is timing, prijsactie en operational readiness goed genoeg om te handelen?

Een markt kan context hebben zonder setup. Een setup kan bestaan zonder execution-ready te zijn.

---

## 3. DEFINITIE VAN A SETUP

Een A-setup is een inhoudelijk valide, maar niet maximale, DET-kans. Een A-setup vereist minimale kwaliteit op context, structuur en trigger, maar staat nog enige imperfectie toe zolang de kernlogica intact blijft.

### Minimale voorwaarden voor A
De volgende voorwaarden moeten minimaal aanwezig zijn:
- duidelijke directionele bias;
- bruikbare relatie tot VWAP;
- voldoende 5m-context;
- geen hard blocker;
- bruikbare 1m-trigger;
- voldoende structurele ruimte voor een DET-entry;
- geen evidente chase of rommelige random entry.

### Benodigde 5m-context voor A
Voor A moet op 5m minimaal aanwezig zijn:
- bias in dezelfde richting als de kandidaat-trade;
- VWAP-richting die de trade niet tegenspreekt;
- VWAP mag niet flat of betekenisloos zijn;
- markt mag niet duidelijk choppy of directionloos zijn;
- structuur moet voldoende directionele orde tonen;
- de move mag niet duidelijk uitgeput of te ver doorgelopen zijn.

De 5m-context hoeft niet perfect te zijn, maar moet wel duidelijk bruikbaar zijn.

### Benodigde relatie tot VWAP voor A
Voor A geldt:
- prijs moet logisch gepositioneerd zijn ten opzichte van VWAP;
- de trade moet passen bij acceptatie, rejectie of continuatie rond VWAP;
- een setup mag niet volledig losgezongen zijn van VWAP-context;
- een beperkte uitbreiding weg van VWAP kan acceptabel zijn zolang timing en structuur nog vers blijven.

### Benodigde 1m-bevestiging voor A
Voor A moet op 1m een bruikbare bevestiging zichtbaar zijn, zoals:
- rejectie van een ongunstige zijde;
- reclaim en hold in de trade-richting;
- gecontroleerde pullback die geen structurele breuk veroorzaakt;
- beginnende of duidelijke re-acceleratie;
- candlekwaliteit die laat zien dat de markt de gekozen richting actief ondersteunt.

### Belangrijke wick-, rejectie- en re-acceleratie-elementen voor A
Voor A zijn de volgende elementen belangrijk:
- wick-behavior ondersteunt de richting in plaats van deze te ontkennen;
- rejecties moeten betekenisvol zijn en niet volledig tegenstrijdig;
- een re-acceleratie mag aanwezig zijn maar hoeft niet uitzonderlijk krachtig te zijn;
- de entry mag na een imperfecte, maar nog bruikbare retest ontstaan.

### Wat nog acceptabel is voor A maar niet voor A+
Een A-setup mag nog bevatten:
- lichte rommeligheid in de opbouw;
- minder perfecte alignment tussen alle contextlagen;
- een iets minder schone 1m-trigger;
- iets minder overtuigende wick- of bodykwaliteit;
- een setup die goed genoeg is maar niet uitzonderlijk helder.

A is dus een valide setup met duidelijke kwaliteit, maar zonder de extra zuiverheid en alignment die A+ definieert.

---

## 4. DEFINITIE VAN A+ SETUP

Een A+-setup is een uitzonderlijk sterke DET-kans met hoge alignment tussen context, structuur en trigger.

### Wat A+ sterker maakt dan A
A+ vereist niet alleen valide context, maar ook uitzonderlijk schone samenloop van factoren:
- sterkere bias;
- schonere structuur;
- scherpere VWAP-relatie;
- betere timing;
- duidelijkere 1m-bevestiging;
- minder twijfel in locatie, risico en entrykwaliteit.

### Extra alignment voor A+
Voor A+ moet aanvullend aanwezig zijn:
- sterke overeenkomst tussen trend, VWAP-richting en context;
- duidelijke acceptatie in de trade-richting;
- weinig tot geen tegenstrijdige contextsignalen;
- hogere kwaliteit in candle location en continuation-kwaliteit.

### Schonere structuur voor A+
Voor A+ moet de marktstructuur schoner zijn dan bij A:
- minder rommeligheid;
- duidelijker directional sequencing;
- minder twijfelachtige sweeps of random oscillatie;
- hogere kwaliteit van hold, rejectie en vervolg.

### Betere timing voor A+
Voor A+ geldt:
- entry komt niet laat in een uitgeputte move;
- trigger komt op een logisch en fris moment;
- de markt hoeft niet gered te worden door interpretatie;
- de setup oogt direct leesbaar en handelbaar.

### Uitzonderlijk goede context voor A+
Voor A+ moet de context niet alleen valide zijn, maar overtuigend:
- duidelijke VWAP-bias;
- duidelijke directional intent;
- schoon onderscheid tussen context en ruis;
- sterkere samenloop tussen 5m en 1m;
- weinig reden om de setup in SHADOW te zetten.

A+ is dus geen lichte upgrade van A, maar een duidelijk hogere kwaliteitsklasse.

---

## 5. HARD BLOCKERS

Een hard blocker leidt automatisch tot `REJECT`. Zodra een hard blocker aanwezig is, mag de kandidaat niet als executeerbare setup worden geclassificeerd.

### Chop
Automatisch `REJECT` wanneer de markt inhoudelijk geen bruikbare directional context heeft, bijvoorbeeld:
- prijsactie is random en oscillatief;
- VWAP-context is niet richtinggevend;
- compressie is niet gezond maar rommelig;
- candles spreken elkaar structureel tegen;
- er is geen bruikbaar onderscheid tussen pullback en ruis.

### Late
Automatisch `REJECT` wanneer de move inhoudelijk te laat is, bijvoorbeeld:
- prijs is duidelijk ver geëxpandeerd weg van VWAP;
- meerdere pushes hebben de move uitgeput;
- entry komt in extensie in plaats van in structuur;
- er is geen frisse reclaim/pullback/retest meer;
- resterende structuur ondersteunt geen kwalitatieve DET-entry meer.

### Slechte structuur
Automatisch `REJECT` wanneer:
- de markt geen bruikbare directional structuur toont;
- de 5m-context intern tegenstrijdig is;
- highs/lows, closes en acceptatiegedrag geen betrouwbare richting suggereren;
- de trade alleen mogelijk is via forceren of subjectieve interpretatie.

### Geen duidelijke VWAP-bias
Automatisch `REJECT` wanneer:
- VWAP-richting afwezig of betekenisloos is;
- prijs geen bruikbare relatie heeft tot VWAP;
- de setup niet logisch kan worden beschreven vanuit VWAP-context.

### Tegenstrijdige context
Automatisch `REJECT` wanneer:
- trend, VWAP, structuur en trigger elkaar fundamenteel tegenspreken;
- meerdere kernlagen verschillende richtingen suggereren;
- de trade alleen mogelijk wordt door één positief element te negeren tegenover meerdere negatieve elementen.

### Te ver doorgelopen move
Automatisch `REJECT` wanneer:
- de move te ver is gevorderd;
- de kandidaat instapt na het duidelijke kernmoment;
- risico, locatie en vervolgpotentieel niet meer passen bij DET-kwaliteit.

### Onduidelijke 1m-trigger
Automatisch `REJECT` wanneer:
- er geen echte 1m-bevestiging is;
- wick- of rejectiegedrag tegenstrijdig is;
- er geen duidelijke reclaim, rejectie of re-acceleratie is;
- de entry meer op hopen dan op zien berust.

---

## 6. SOFT BLOCKERS / SHADOW-SITUATIES

`SHADOW` is bedoeld voor kandidaten die niet direct executeerbaar zijn, maar inhoudelijk wel leerwaarde of analysekracht hebben.

### Wanneer SHADOW passend is
Een kandidaat hoort in `SHADOW` wanneer:
- context grotendeels valide is maar één laag nog twijfelachtig is;
- de setup inhoudelijk interessant is maar niet schoon genoeg voor execution;
- de trigger gedeeltelijk aanwezig is maar onvoldoende overtuigend;
- de markt borderline is tussen bruikbaar en afwijzen;
- het waardevol is om de kandidaat later terug te analyseren.

### Borderline situaties voor SHADOW
Voorbeelden van SHADOW-situaties:
- bruikbare context maar iets te compacte marktkwaliteit;
- context en structuur kloppen, maar timing is net niet fris genoeg;
- 5m-setup is bruikbaar maar 1m-trigger is te zwak voor execute;
- A-achtige context, maar onvoldoende overtuigende entrykwaliteit;
- situaties die inhoudelijk niet duidelijk fout zijn, maar ook niet execution-waardig.

### Wat SHADOW expliciet niet is
`SHADOW` is geen verborgen execute-klasse. Het is een leer- en observatieklasse. Kandidaten in SHADOW mogen niet automatisch live uitgevoerd worden.

---

## 7. CONTEXTLAGEN

### 7.1 Sessiecontext
Sessiecontext bepaalt of het marktmoment DET-waardig is.

Belangrijk:
- opening en vroege sessiefase hebben hogere prioriteit;
- context buiten relevante sessiemomenten vereist strengere beoordeling;
- lage sessiekwaliteit verlaagt de kans op A of A+.

### 7.2 VWAP-context
VWAP-context bepaalt directional referentie en locatiekwaliteit.

Belangrijk:
- richting van VWAP;
- afstand tot VWAP;
- acceptatie rond VWAP;
- rejectie of hold rond VWAP;
- of prijs in structuur of in extensie handelt.

### 7.3 5m structuurcontext
De 5m structuurcontext bepaalt of de markt een bruikbaar DET-kader biedt.

Belangrijk:
- directional orde;
- closes en candle location;
- sweep/rejectie-gedrag;
- relatieve netheid of rommeligheid;
- bruikbaarheid van de markt voor een vervolg op 1m.

### 7.4 1m triggercontext
De 1m-triggercontext bepaalt of de entry werkelijk zichtbaar wordt.

Belangrijk:
- wick-kwaliteit;
- rejectie;
- reclaim en hold;
- timing van continuation;
- re-acceleratie;
- afwezigheid van tegenstrijdige microstructuur.

### 7.5 Marktkwaliteit / chop
Marktkwaliteit bepaalt of compactie gezond of destructief is.

Belangrijk:
- onderscheid tussen gezonde compressie en echte chop;
- compact betekent niet automatisch onbruikbaar;
- directionele compressie kan DET-bruikbaar zijn;
- random oscillatie zonder directional hold is onbruikbaar.

### 7.6 Late move / moved away
Late move en moved away zijn gerelateerd maar niet identiek.

- `moved_away` kan positief zijn wanneer de markt laat zien dat prijs zich werkelijk in de bedoelde richting verplaatst heeft.
- `late` wordt negatief wanneer die verplaatsing al te ver is doorgeschoten en niet meer fris is.

Het onderscheid moet inhoudelijk behouden blijven:
- bruikbaar vervolg na beweging is mogelijk;
- uitgeputte chase na te veel beweging is onbruikbaar.

### 7.7 Structure / setup / trigger onderscheid
- **Structure**: is de markt op hogere contextlaag voldoende ordelijk en directioneel?
- **Setup**: is er inhoudelijk een tradekandidaat van voldoende kwaliteit?
- **Trigger**: is er nu een concreet en zichtbaar entrymoment?

Deze drie begrippen mogen niet door elkaar worden gehaald.

---

## 8. DET-BESLISBOOM

De inhoudelijke DET-beslisboom luidt als volgt:

1. **Geen bruikbare sessie- of VWAP-context**  
   Uitkomst: `REJECT`

2. **Wel ruwe context, maar geen duidelijke 5m directional structuur**  
   Uitkomst: `REJECT` of `SHADOW` afhankelijk van de mate van bruikbaarheid

3. **Wel 5m-context, maar marktkwaliteit is inhoudelijk choppy of context is fundamenteel tegenstrijdig**  
   Uitkomst: `REJECT`

4. **Wel context, maar move is te laat of te ver geëxpandeerd**  
   Uitkomst: `REJECT`

5. **Wel context en bruikbare structuur, maar 1m-trigger is onvoldoende duidelijk**  
   Uitkomst: `SHADOW`

6. **Wel context, wel setup, wel bruikbare 1m-trigger, maar kwaliteit is goed doch niet uitzonderlijk**  
   Uitkomst: `EXECUTE_A`

7. **Wel context, wel setup, wel sterke 1m-trigger, en alignment is uitzonderlijk schoon**  
   Uitkomst: `EXECUTE_A_PLUS`

De beslisboom is hiërarchisch: context gaat vóór setup, setup gaat vóór trigger, trigger gaat vóór execution-classificatie.

---

## 9. MAPPING NAAR PINE / TV FEATURES

### `htf_trend_up/down`
**Betekenis binnen DET:** richting van hogere timeframe-context.  
**Type:** contextueel, zwaarwegend.  
**Weging:** hoog, maar niet volledig op zichzelf beslissend. Trend mag niet tegenspreken; trend moet de setup ondersteunen.

### `htf_vwap_up/down`
**Betekenis binnen DET:** richting van VWAP-bias.  
**Type:** contextueel, zwaarwegend.  
**Weging:** zeer hoog. Zonder duidelijke VWAP-richting geen DET-kwaliteit.

### `htf_vwap_not_flat`
**Betekenis binnen DET:** VWAP is directioneel bruikbaar en niet betekenisloos vlak.  
**Type:** contextueel / hard.  
**Weging:** hoog. Een vlakke of betekenisloze VWAP-context ondermijnt DET.

### `htf_not_choppy`
**Betekenis binnen DET:** markt is niet destructief rommelig.  
**Type:** hard of soft, afhankelijk van de ernst.  
**Weging:** hoog. Niet elke compactie is chop; de interpretatie moet zorgvuldig gebeuren.

### `htf_not_late_long/short`
**Betekenis binnen DET:** de move is nog niet inhoudelijk te laat.  
**Type:** hard.  
**Weging:** zeer hoog. Een late chase is geen DET-entry.

### `htf_moved_away_long/short`
**Betekenis binnen DET:** prijs heeft zich bruikbaar in de bedoelde richting ontwikkeld.  
**Type:** contextueel.  
**Weging:** middel-hoog. Positief wanneer het richting valideert, negatief wanneer het samen met late op uitputting wijst.

### `htf_structure_long/short`
**Betekenis binnen DET:** hogere timeframe-structuur ondersteunt de trade inhoudelijk.  
**Type:** zwaar contextueel / mogelijk hard.  
**Weging:** zeer hoog. Zwakke structuur verlaagt direct de setupkwaliteit.

### `htf_setup_long/short`
**Betekenis binnen DET:** de samenvattende 5m-setupconditie is aanwezig.  
**Type:** samenvattend context/setup-veld.  
**Weging:** hoog, maar moet uitlegbaar blijven via onderliggende factoren.

### `distance_from_vwap_atr`
**Betekenis binnen DET:** mate van uitbreiding weg van VWAP in volatiliteitsgenormaliseerde termen.  
**Type:** contextueel / blocker-ondersteunend.  
**Weging:** hoog als waarschuwing voor late/extensie, maar niet als enige definitie van late.

### `htf_ema_spread_atr`
**Betekenis binnen DET:** mechanische maat voor expansie of compactie.  
**Type:** contextueel / ondersteunend.  
**Weging:** middel. Bruikbaar als hulpmiddel, maar onvoldoende als enige definitie van chop.

### `body_strength`
**Betekenis binnen DET:** kwaliteit van candle-body als teken van directional intent.  
**Type:** trigger- en setup-ondersteunend.  
**Weging:** middel-hoog. Sterke bodies ondersteunen continuation en acceptatie.

### `blocker`
**Betekenis binnen DET:** door TradingView gemelde primaire blokkade.  
**Type:** informatief / diagnostisch.  
**Weging:** niet autonoom beslissend. De bot mag blocker meenemen, maar moet inhoudelijk blijven beoordelen.

---

## 10. CLASSIFICATIE-UITKOMSTEN VOOR DE BOT

### `REJECT`
De kandidaat is inhoudelijk niet DET-waardig. Er is onvoldoende context, een hard blocker is actief, of setup/triggerkwaliteit is onvoldoende.

### `SHADOW`
De kandidaat is inhoudelijk interessant maar niet direct executeerbaar. Logging en latere evaluatie zijn wenselijk. Geen live execution.

### `EXECUTE_A`
De kandidaat voldoet inhoudelijk aan een valide A-setup:
- context is bruikbaar;
- geen hard blocker;
- structuur en trigger zijn voldoende;
- kwaliteit is goed maar niet uitzonderlijk.

### `EXECUTE_A_PLUS`
De kandidaat voldoet inhoudelijk aan een uitzonderlijk sterke A+-setup:
- context is overtuigend;
- alignment is sterk;
- structuur is schoon;
- trigger is hoogwaardig;
- weinig of geen inhoudelijke twijfel.

---

## 11. WAT NOG NIET AUTOMATISCH MAG

De volgende onderdelen mogen nog niet volledig automatisch live execution vrijgeven:
- borderline interpretatie van chop;
- borderline interpretatie van late;
- subjectieve verschillen tussen gezonde compactie en random chop;
- subjectieve verschillen tussen A en A+ in twijfelgevallen;
- execution-vrijgave zonder voldoende shadow-validatie.

De volgende onderdelen vragen nog menselijk oordeel:
- review van borderline SHADOW-cases;
- evaluatie of een proxy inhoudelijk DET-getrouw genoeg is;
- beoordeling van false negatives en false positives;
- beoordeling van uitzonderlijke marktomstandigheden.

De volgende onderdelen moeten eerst shadow-mode blijven:
- nieuwe classificatieregels;
- nieuwe soft blocker-logica;
- nieuwe A/A+ scheidslijnen;
- iedere logicawijziging die nog niet op representatieve batches is gevalideerd.

---

## 12. IMPLEMENTATIE-RICHTLIJNEN VOOR DE SCALPINGBOT

### Wat eerst gebouwd moet worden
Eerste prioriteit:
- bot-side decision layer die enriched TradingView payloads inhoudelijk leest;
- classificatie naar `REJECT`, `SHADOW`, `EXECUTE_A`, `EXECUTE_A_PLUS`;
- volledige logging van inputvelden, interne beoordeling en eindclassificatie;
- vergelijking van TradingView-blockers met bot-side DET-beslissing.

### Wat later pas mag
Pas in latere fase:
- automatische execution-vrijgave op basis van classification outcome;
- automatische position sizing op basis van definitieve A/A+ confidence;
- live deployment zonder shadow-validatie;
- autonome behandeling van twijfelgevallen zonder review.

### Veilige volgorde
Veilige implementatievolgorde:
1. enriched payload ontvangen en loggen;
2. bot-side inhoudelijke classificatie bouwen;
3. classification-only mode draaien;
4. shadow-analyse en batch review;
5. pas daarna beperkte execution-koppeling overwegen;
6. pas daarna conditionele live release voor strikt gevalideerde scenario’s.

De heilige architectuur blijft ongewijzigd:
TradingView -> Webhook -> Queue -> Worker -> IBKR

---

## KORTE SAMENVATTING

Dit document definieert het DET-beslismodel als inhoudelijke beoordelingslaag binnen de IBKR scalpingbot. TradingView detecteert kandidaten en levert enriched context aan. De bot beslist vervolgens of een kandidaat moet worden afgewezen, in shadow-mode moet blijven, of inhoudelijk voldoet aan A- of A+-kwaliteit. De kern van het model is VWAP-centrisch, context-gedreven en streng op kwaliteit. Context, setup en trigger zijn expliciet gescheiden. Het model is in deze fase bedoeld voor classificatie en validatie, nog niet voor onbeperkte autonome live execution.

---

## DE 10 BELANGRIJKSTE DET-BESLISREGELS

1. Zonder duidelijke VWAP-bias is er geen DET-setup.
2. Zonder valide 5m-context is er geen trade-kandidaat.
3. Een goede context zonder duidelijke 1m-trigger is geen execution-waardige setup.
4. Geen trade is binnen DET een volledig correcte uitkomst.
5. Late extensie is een hard afwijscriterium wanneer de move inhoudelijk niet meer fris is.
6. Chop moet inhoudelijk worden beoordeeld; niet elke compactie is onbruikbaar, maar echte random marktkwaliteit is een hard blocker.
7. Structure, setup en trigger zijn drie aparte lagen en mogen niet worden samengevoegd.
8. A vereist duidelijke kwaliteit; A+ vereist uitzonderlijk schone alignment.
9. SHADOW is een leer- en observatieklasse, geen verborgen execute-klasse.
10. Nieuwe decision logic mag pas richting execution opschuiven na representatieve shadow-validatie.