--------------------------- google adwords ---------------------------
create table if not exists adwords_stat (
campaign_id bigint not NULL, -- идентификатор рекланмой кампании
ad_id bigint NULL,  -- идентификатор объявления
criteria_id bigint NULL, -- идентификатор ключевого слова / таргетинга
adset_id bigint NULL, -- идентификатор группы объявлений
dt date not null,  -- дата
impressions numeric, -- показы рекланмых объявлений
clicks numeric, -- клики по рекламным объявлениям
spends numeric, -- расходы по рекламному объявлению (без НДС)
spends_gross numeric, -- расходы по рекламному объвялению (с НДС)
currency text, -- валюта расходов
source text not null DEFAULT 'adwords', -- название источника (автоматически ставится базой)
created_at timestamp not null default now()
);

create table if not exists adwords_campaigns (
campaign_id bigint not NULL, -- идентификаторы рекламной кампании
campaign_name text not NULL, -- название рекламной кампании 
campaign_type text, -- тип рекламной кампании // надо посмотреть что есть
campaign_status text, -- статус рекланмной кампании (active, stoped, archived)
account_id bigint, -- или логин, или идентификатор рекламного кабинета 
account_name text, -- название рекланмого кабинета
source text not null DEFAULT 'adwords', -- название источника (автоматически ставится базой)
created_at timestamp DEFAULT now()
);

create table if not exists adwords_ads (
ad_id bigint not NULL, -- id рекламного объвления
ad_name text not NULL, -- названре рекламного объявления
ad_type text, -- тип рекламного объявлеия
campaign_id bigint not NULL, 
url text, -- Final url| ссылка на страницу (куда ведет объявление) | ПРВОВЕРИТЬ
utm_source text, -- get parametrs from url
utm_medium text,-- get parametrs from url
utm_campagin text,-- get parametrs from url
utm_term text,-- get parametrs from url
utm_content text,-- get parametrs from url
source text not null DEFAULT 'adwords', -- название источника (автоматически ставится базой)
created_at timestamp not null default now()
);

create table if not exists adwords_criteria (
criteria_id bigint not NULL, -- id таргетинга
criteria text not NULL, -- таргетинг (ключевое слово или сегмент)
criteria_type text, -- тип таргетинга | OPTIONAL
campaign_id bigint not NULL, 
url text, -- Final url| ссылка на страницу (куда ведет объявление) | ПРВОВЕРИТЬ
utm_source text, -- get параметры из URL
utm_medium text,-- get parametrs from url
utm_campagin text,-- get parametrs from url
utm_term text,-- get parametrs from url
utm_content text,-- get parametrs from url
source text not null DEFAULT 'adwords', -- название источника (автоматически ставится базой)
created_at timestamp not null default now()
);
--------------------------- google adwords ---------------------------

--------------------------- facebook_ads ---------------------------
create table if not exists fb_marketing_stat (
campaign_id bigint not NULL, -- идентификатор рекланмой кампании
ad_id bigint NULL,  -- идентификатор объявления
criteria_id bigint NULL, -- идентификатор ключевого слова / таргетинга
adset_id bigint NULL, -- идентификатор группы объявлений
dt date not null,  -- дата
impressions numeric, -- показы рекланмых объявлений
clicks numeric, -- клики по рекламным объявлениям
spends numeric, -- расходы по рекламному объявлению (без НДС)
spends_gross numeric, -- расходы по рекламному объвялению (с НДС)
currency text, -- валюта расходов
source text not null DEFAULT 'fb_marketing', -- название источника (автоматически ставится базой)
created_at timestamp not null default now()
);

create table if not exists fb_marketing_campaigns (
campaign_id bigint not NULL, -- идентификаторы рекламной кампании
campaign_name text not NULL, -- название рекламной кампании 
campaign_type text, -- тип рекламной кампании // надо посмотреть что есть
campaign_status text, -- статус рекланмной кампании (active, stoped, archived)
account_id bigint, -- или логин, или идентификатор рекламного кабинета 
account_name text, -- название рекланмого кабинета
source text not null DEFAULT 'fb_marketing', -- название источника (автоматически ставится базой)
created_at timestamp DEFAULT now()
);

create table if not exists fb_marketing_ads (
ad_id bigint not NULL, -- id рекламного объвления
ad_name text not NULL, -- названре рекламного объявления
ad_type text, -- тип рекламного объявлеия
campaign_id bigint not NULL, 
url text, -- Final url| ссылка на страницу (куда ведет объявление) | ПРВОВЕРИТЬ
utm_source text, -- get parametrs from url
utm_medium text,-- get parametrs from url
utm_campagin text,-- get parametrs from url
utm_term text,-- get parametrs from url
utm_content text,-- get parametrs from url
source text not null DEFAULT 'fb_marketing', -- название источника (автоматически ставится базой)
created_at timestamp not null default now()
);

create table if not exists fb_marketing_criteria (
criteria_id bigint not NULL, -- id таргетинга
criteria text not NULL, -- таргетинг (ключевое слово или сегмент)
criteria_type text, -- тип таргетинга | OPTIONAL
campaign_id bigint not NULL, 
url text, -- Final url| ссылка на страницу (куда ведет объявление) | ПРВОВЕРИТЬ
utm_source text, -- get параметры из URL
utm_medium text,-- get parametrs from url
utm_campagin text,-- get parametrs from url
utm_term text,-- get parametrs from url
utm_content text,-- get parametrs from url
source text not null DEFAULT 'fb_marketing', -- название источника (автоматически ставится базой)
created_at timestamp not null default now()
);
--------------------------- facebook_ads ---------------------------