create table stg_news_items
(
    id           serial
        primary key,
    abstract     varchar(2047),
    content      text,
    title        varchar(4095),
    image_src    varchar(2047),
    published_at timestamp,
    url          varchar                                not null
        constraint uq_stg_news_items_url
            unique,
    created_at   timestamp default CURRENT_TIMESTAMP(0) not null,
    updated_at   timestamp default CURRENT_TIMESTAMP(0) not null
);
