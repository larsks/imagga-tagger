create table photos (
    id integer primary key,
    path text,

    constraint path_unique unique (path)
);

create table tags (
    id integer primary key,
    tag_name varchar(40),

    constraint tag_name_unique unique (tag_name)
);

create table photo_tag (
    photo_id integer references photos(id),
    tag_id integer references tags(id),
    confidence float
);
