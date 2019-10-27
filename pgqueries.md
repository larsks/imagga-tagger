# Example JSON queries

The follow examples all assume:

```python
>>> import sqlalchemy
>>> from tabulate import tabulate
>>> db = sqlalchemy.create_engine("postgresql://photos:secret@127.0.0.1/photos")
>>>
```

## Find photos that might be cats

```python
>>> res = db.execute("select count(path) from photos where data @? '$.tags.cat'")
>>> res.fetchall()
[(15,)]
>>>
```

The above query uses a *jsonpath* expression, which is only available in Postgres 12 and later. The following query is equivalent and will work with both Postgres 11 and 12:

```python
>>> res = db.execute("select count(path) from photos where data -> 'tags' ? 'cat'")
>>> res.fetchall()
[(15,)]
>>>
```

## Find photos of cats in boxes

This looks for photos that were tagged as both `cat` and `box`.

```python
>>> res = db.execute("select count(path) from photos where data -> 'tags' ?& array['box', 'cat']")
>>> res.fetchall()
[(1,)]
>>>
```

## Find all photos that are probably cats

This looks for photos that were categorized as `cat` with a high level of confidence.

```python
>>> res = db.execute("select path, data #> '{tags,cat}' from photos "
... "where (data #> '{tags,cat}')::float > 80")
>>> print(tabulate(res.fetchall()))
------------------------------------------------------------------------  --------
/mnt/photos/photos_by_id/b4/b47b7d64d9cb8bd31a665c69885934714f6742a3.jpg  100
/mnt/photos/photos_by_id/27/27491b57b8d266bae635c33b58b9c4ff5c3e2a60.jpg  100
/mnt/photos/photos_by_id/2d/2d31a8bb7c5d4c3f2b9c47c7faebe2044068b168.jpg  100
/mnt/photos/photos_by_id/2d/2d5b8440ab9da79dacec3482084752a23b1709a1.jpg   92.4775
------------------------------------------------------------------------  --------
>>>
```

## Find all high confidence tags for a given photo

Find all tags for a given photo where the confidence is `>=` 40.

```python
>>> res = db.execute("select jsonb_path_query_array(data, "
... "'$.tags.keyvalue() ? (@.value >= 40)') from photos where "
... "path = '/mnt/photos/photos_by_id/b4/b40683006965c8203400d301ec3a12484c6ce676.jpg'");
>>> print(tabulate({x['key']: x['value'] for x in res.fetchone()[0]}.items()))
---------  --------
area       100
home        64.7597
room       100
house       63.6555
modern      51.9955
kitchen    100
interior    76.2474
furniture   47.7479
---------  --------
>>>
```
