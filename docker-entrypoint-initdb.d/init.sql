create table prices (
    id serial primary key,
    symbol text, 
    date timestamptz,
    open numeric(12,6), 
    high numeric(12,6), 
    low numeric(12,6), 
    close numeric(12,6), 
    volume bigint
);


create table signals (
    id serial primary key,
    symbol text,
    date timestamptz,
    signal_type varchar(8),
    price numeric(12,6),
    shares integer
);


create table portfolio_history (
    id serial primary key,
    date timestamptz,
    cash numeric(18,6),
    positions_value numeric(18,6),
    total_value numeric(18,6)
);
