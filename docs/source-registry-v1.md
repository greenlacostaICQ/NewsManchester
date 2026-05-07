# Source Registry v1.1

Рабочий реестр источников для `Greater Manchester AM Brief`.

Это не плоский список “всё сразу”, а staged registry для реального запуска.

## 1. Поля для каждого источника

У каждого источника в registry обязательно есть:

- `Name`
- `Category`
- `Tier`
- `Type`
- `Batch`
- `URL`
- `Notes`

## 2. Типы источников

- `rss`
  - стабильный feed, приоритетный тип для MVP
- `api`
  - структурированные данные, лучший вариант для транспорта и погоды
- `html`
  - обычная страница, использовать если нет RSS или API
- `signal`
  - ранний сигнал, никогда не единственное основание для финального пункта

Правило:

- `rss` и `api` — default выбор
- `html` — только если `rss/api` нет
- `signal` — never primary

## 3. Batch-окна для morning brief

Пайплайн не делает hourly polling весь день. Для ежедневного выпуска достаточно трёх волн:

- `Wave A — 06:00`
  - тяжёлые и более медленные источники
  - councils
  - media
  - universities
  - venues
  - openings
- `Wave B — 07:00`
  - максимально свежие источники
  - TfGM
  - National Rail
  - Met Office
  - football fixtures/results
  - GMP
- `Wave C — 07:15`
  - только если сегодня есть presale / general sale / критичный transport update
  - ticket refresh
  - second pass по transport

## 4. Phase 1 MVP

Это стартовый набор, который уже даёт живой выпуск.

### Transport / weather / safety

#### TfGM / Bee Network

- Category: `transport`
- Tier: `primary`
- Type: `api`
- Batch: `Wave B`
- URL: `https://developer.tfgm.com/`
- Notes: приоритетный источник по Metrolink, Bee Network bus, travel alerts

#### National Rail

- Category: `transport`
- Tier: `primary`
- Type: `html`
- Batch: `Wave B`
- URL: `https://www.nationalrail.co.uk/service-disruptions/`
- Notes: для MVP можно брать disruption pages; затем перейти на Darwin API

#### Met Office

- Category: `weather`
- Tier: `primary`
- Type: `api`
- Batch: `Wave B`
- URL: `https://www.metoffice.gov.uk/services/data`
- Notes: использовать для температуры и осадков; без поисковых страниц

#### GMP Newsroom

- Category: `safety`
- Tier: `primary`
- Type: `rss`
- Batch: `Wave B`
- URL: `https://www.gmp.police.uk/news/greater-manchester/news/news/`
- Notes: только city-impact incidents, не жёлтая криминалка

### Councils

#### Manchester Council

- Category: `council`
- Tier: `primary`
- Type: `rss`
- Batch: `Wave A`
- URL: `https://www.manchester.gov.uk/news`
- Notes: главный civic слой по Manchester

#### Salford Council

- Category: `council`
- Tier: `primary`
- Type: `rss`
- Batch: `Wave A`
- URL: `https://news.salford.gov.uk/news/`
- Notes: один из самых высокоотдающих council-источников

### Football

#### Manchester United

- Category: `football`
- Tier: `primary`
- Type: `rss`
- Batch: `Wave B`
- URL: `https://www.manutd.com/en/news`
- Notes: результаты, preview, squad news

#### Manchester City

- Category: `football`
- Tier: `primary`
- Type: `rss`
- Batch: `Wave B`
- URL: `https://www.mancity.com/news`
- Notes: результаты, preview, squad news

#### Salford City

- Category: `football`
- Tier: `primary`
- Type: `html`
- Batch: `Wave B`
- URL: `https://www.salfordcityfc.co.uk/news/`
- Notes: менее удобный, но нужен для local football layer

### Events / culture / tickets

#### Co-op Live

- Category: `tickets`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.cooplive.com/events`
- Notes: major arena shows, presale windows, venue-first source

#### AO Arena

- Category: `tickets`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.ao-arena.com/events`
- Notes: major arena shows

#### Factory International

- Category: `culture`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://factoryinternational.org/whats-on/`
- Notes: выставки, performances, talks

### Media / openings

#### Manchester Evening News

- Category: `media`
- Tier: `secondary`
- Type: `rss`
- Batch: `Wave A`
- URL: `https://www.manchestereveningnews.co.uk/`
- Notes: broad local coverage; нужен жёсткий significance filter

#### BBC Manchester

- Category: `media`
- Tier: `secondary`
- Type: `rss`
- Batch: `Wave A`
- URL: `https://www.bbc.com/news/england/manchester`
- Notes: high-trust regional coverage

#### The Mill

- Category: `media`
- Tier: `secondary`
- Type: `rss`
- Batch: `Wave A`
- URL: `https://manchestermill.co.uk/`
- Notes: housing, civic life, politics, deeper context

#### ITV News Granada

- Category: `media`
- Tier: `secondary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.itv.com/news/granada`
- Notes: strong regional layer for incidents, courts, NHS and transport follow-ups

#### The Manc

- Category: `media`
- Tier: `secondary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://themanc.com/`
- Notes: future announcements, theatre, culture, weekend planning and big city events

#### Manchester's Finest

- Category: `openings`
- Tier: `secondary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.manchestersfinest.com/`
- Notes: openings, food, bars, hospitality signals

## 5. Phase 2

Расширение после стабилизации MVP.

### Additional councils

#### Trafford Council

- Category: `council`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.trafford.gov.uk/news/`
- Notes: Phase 2A waiver for now — live page, but current article-card markup does not expose links in a shape the generic HTML extractor picks up reliably

#### Stockport Council

- Category: `council`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.stockport.gov.uk/newsroom`
- Notes: Phase 2A waiver for now — server-rendered newsroom exposes no usable article links or feed in the initial HTML

#### Oldham Council

- Category: `council`
- Tier: `primary`
- Type: `rss`
- Batch: `Wave A`
- URL: `https://www.oldham.gov.uk/rss/news`
- Notes: live in collector, usable RSS with district-level civic updates

#### Rochdale Council

- Category: `council`
- Tier: `primary`
- Type: `rss`
- Batch: `Wave A`
- URL: `https://www.rochdale.gov.uk/rss/news`
- Notes: live in collector, usable RSS with district-level civic updates

#### Bolton Council

- Category: `council`
- Tier: `primary`
- Type: `rss`
- Batch: `Wave A`
- URL: `https://www.bolton.gov.uk/rss/news`
- Notes: live in collector, usable RSS with district-level civic updates

#### Tameside Council

- Category: `council`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.tameside.gov.uk/newsroom`
- Notes: Phase 2A waiver for now — newsroom path is live but exposes no usable server-rendered article links or feed

#### Bury Council

- Category: `council`
- Tier: `primary`
- Type: `rss`
- Batch: `Wave A`
- URL: `https://www.mynewsdesk.com/uk/rss/current_news/49585`
- Notes: live in collector through Bury Council Mynewsdesk newsroom RSS

#### Wigan Council

- Category: `council`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.wigan.gov.uk/News/News.aspx`
- Notes: district-level civic updates

### Theatres / arts / museums

#### Visit Manchester

- Category: `events`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.visitmanchester.com/whats-on`
- Notes: broad discovery layer for weekly planning

#### The Lowry

- Category: `culture`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://thelowry.com/whats-on`
- Notes: live in collector, usable event-listing path

#### HOME

- Category: `culture`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://homemcr.org/whats-on/`
- Notes: cinema, theatre, exhibitions

#### Palace Theatre / Opera House

- Category: `culture`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.atgtickets.com/venues/palace-theatre-manchester/whats-on/`
- Notes: Phase 2B waiver for now — landing page exposes only genre/category links, not usable event pages

#### Royal Exchange Theatre

- Category: `culture`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.royalexchange.co.uk/whats-on-manchester/`
- Notes: Phase 2B waiver for now — live page exposes no usable server-rendered event links to the generic extractor

#### Contact Theatre

- Category: `culture`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://contactmcr.com/whats-on/`
- Notes: theatre and youth-led programme with city relevance

#### Whitworth

- Category: `museum`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.whitworth.manchester.ac.uk/whats-on/`
- Notes: exhibitions with long city relevance

#### Science and Industry Museum

- Category: `museum`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.scienceandindustrymuseum.org.uk/whats-on/`
- Notes: Phase 2B waiver for now — repeated collector fetches return HTTP 405 despite the public listing being visible in a browser

#### Manchester Art Gallery

- Category: `museum`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://manchesterartgallery.org/whats-on/`
- Notes: Phase 2B waiver for now — returns HTTP 403 to collector fetches

#### People's History Museum

- Category: `museum`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://phm.org.uk/whats-on/`
- Notes: exhibitions and public events with civic relevance

#### John Rylands Library

- Category: `museum`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.library.manchester.ac.uk/rylands/visit/events/`
- Notes: exhibitions, talks and special programming

### Tech / business / universities

#### Manchester Digital

- Category: `tech`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.manchesterdigital.com/`
- Notes: live in collector, constrained to `/post/manchester-digital/` for tech/business signal

#### Prolific North

- Category: `business`
- Tier: `secondary`
- Type: `rss`
- Batch: `Wave A`
- URL: `https://www.prolificnorth.co.uk/news/?feed=rss2`
- Notes: live in collector, constrained to `/news/` plus GM/business signal

#### The Business Desk North West

- Category: `business`
- Tier: `secondary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.thebusinessdesk.com/northwest`
- Notes: deals, M&A, office moves, leadership changes

#### University of Manchester

- Category: `university`
- Tier: `primary`
- Type: `rss`
- Batch: `Wave A`
- URL: `https://www.manchester.ac.uk/about/news/`
- Notes: research, public lectures, city-impact dates

#### Manchester Metropolitan

- Category: `university`
- Tier: `primary`
- Type: `rss`
- Batch: `Wave A`
- URL: `https://www.mmu.ac.uk/news-and-events`
- Notes: events, research, city-impact dates

#### University of Salford

- Category: `university`
- Tier: `primary`
- Type: `rss`
- Batch: `Wave A`
- URL: `https://www.salford.ac.uk/news`
- Notes: innovation, public events, partnerships

### NHS / public services

#### NHS Greater Manchester

- Category: `public_services`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://gmintegratedcare.org.uk/news/`
- Notes: strikes, service changes, urgent health-system updates

#### Greater Manchester Combined Authority

- Category: `public_services`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.greatermanchester-ca.gov.uk/news/`
- Notes: city-region decisions, transport politics, mayoral layer

#### Mayor of Greater Manchester

- Category: `public_services`
- Tier: `secondary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.greatermanchester-ca.gov.uk/who-we-are/the-mayor/news/`
- Notes: statements that can raise the weight of a transport or civic story

### Food / lifestyle / hyperlocal

#### Confidentials

- Category: `openings`
- Tier: `secondary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://confidentials.com/manchester`
- Notes: hospitality and openings

#### I Love Manchester

- Category: `lifestyle`
- Tier: `secondary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://ilovemanchester.com/`
- Notes: openings, city guides, local events

#### Secret Manchester

- Category: `lifestyle`
- Tier: `secondary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://secretmanchester.com/`
- Notes: events, exhibitions, food, markets

#### Manchester Cathedral

- Category: `culture`
- Tier: `secondary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://manchestercathedral.org/whats-on/`
- Notes: concerts, seasonal programming and civic events

#### Altrincham Today

- Category: `hyperlocal`
- Tier: `secondary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://altrincham.todaynews.co.uk/`
- Notes: Trafford and neighbourhood-level signals

#### Altrincham Market

- Category: `food_hall`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://altrinchammarket.co.uk/`
- Notes: resident changes and food openings outside city centre

#### Mackie Mayor

- Category: `food_hall`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://mackiemayor.co.uk/`
- Notes: resident changes and openings

#### Freight Island

- Category: `food_hall`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://www.freightisland.com/`
- Notes: events and resident changes

## 6. Phase 3

Источники, которые полезны, но не должны тормозить MVP.

### Ticket discovery / alternatives

#### Songkick Manchester

- Category: `tickets`
- Tier: `secondary`
- Type: `api`
- Batch: `Wave C`
- URL: `https://www.songkick.com/metro-areas/24475-uk-manchester`
- Notes: полезен как альтернатива тяжёлому ticket scraping

#### Bandsintown Manchester

- Category: `tickets`
- Tier: `secondary`
- Type: `api`
- Batch: `Wave C`
- URL: `https://www.bandsintown.com/`
- Notes: хороший городский discovery layer по анонсам

#### DICE

- Category: `tickets`
- Tier: `secondary`
- Type: `html`
- Batch: `Wave C`
- URL: `https://dice.fm/`
- Notes: только после стабилизации MVP; нужен отдельный anti-bot review

#### See Tickets

- Category: `tickets`
- Tier: `secondary`
- Type: `html`
- Batch: `Wave C`
- URL: `https://www.seetickets.com/`
- Notes: theatre and concerts; запускать точечно

### Airport / intercity

#### Manchester Airport Media Centre

- Category: `airport`
- Tier: `primary`
- Type: `html`
- Batch: `Wave A`
- URL: `https://mediacentre.manchesterairport.co.uk/`
- Notes: delays, route launches, terminal changes; high-yield только в travel peaks

### Signal-only layer

#### Reddit r/manchester

- Category: `signal`
- Tier: `signal-only`
- Type: `signal`
- Batch: `Wave C`
- URL: `https://www.reddit.com/r/manchester/.rss`
- Notes: ранние сигналы по transport и incidents; требует обязательной верификации

#### Local social accounts / X

- Category: `signal`
- Tier: `signal-only`
- Type: `signal`
- Batch: `Wave C`
- URL: `manual registry`
- Notes: использовать только как триггер для проверки, не как источник факта

## 7. No-go и caution

### Не использовать как primary scraping без отдельного решения

#### Ticketmaster

- Status: `caution`
- Reason: Cloudflare, anti-bot, ToS risk
- Preferred alternative: `официальный venue page + Songkick/Bandsintown + manual verification в день sale`

#### DICE

- Status: `caution`
- Reason: нестабильный публичный доступ, возможные anti-bot ограничения
- Preferred alternative: `официальный venue page`

#### Reddit API

- Status: `no-go` для прямой API-интеграции на MVP
- Reason: лишняя сложность и лимиты
- Preferred alternative: `.rss` feed только как signal layer

#### X / Twitter API

- Status: `no-go`
- Reason: дорогой и нестабильный для MVP канал
- Preferred alternative: official newsroom pages и transport alerts

## 8. Практическое правило приоритета

Если нужно быстро собрать живой выпуск, high-yield слой выглядит так:

- TfGM / Bee Network
- National Rail
- Met Office
- Manchester Council
- Salford Council
- GMP
- Manchester United
- Manchester City
- Salford City
- Co-op Live
- AO Arena
- Factory International
- Manchester Evening News
- BBC Manchester
- The Mill
- ITV News Granada
- The Manc
- NHS Greater Manchester
- Manchester's Finest

Эти источники дают основную массу ежедневного выпуска. Всё остальное наращивается фазами.
