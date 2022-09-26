create table dd_market_deal
(
    id           int auto_increment primary key,
    actor        varchar(64)                            not null,
    payload_cid  varchar(128) default ''                not null,
    piece_cid    varchar(128)                           not null,
    deal_id      bigint                                 not null,
    sector_id    bigint                                 not null,
    offset       bigint                                 not null,
    length       bigint                                 not null,
    is_from_user tinyint(1) default 0 not null,
    created_at   timestamp    default CURRENT_TIMESTAMP not null
);

create index dd_market_deal_payload_cid_index
    on dd_market_deal (payload_cid);


