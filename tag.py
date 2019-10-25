#!/usr/bin/env python3

import aiohttp
import asyncio
import click
import json
import logging
import os
import sqlite3

from aiostream import stream, pipe
from pathlib import Path
from PIL import Image

LOG = logging.getLogger(__name__)

PIL_logger = logging.getLogger('PIL')
PIL_logger.setLevel('WARNING')


schema_photos = '''create table if not exists photos (
    id integer primary key,
    path text,

    constraint path_unique unique (path)
);'''

schema_tags = '''create table if not exists tags (
    id integer primary key,
    tag_name varchar(40),

    constraint tag_name_unique unique (tag_name)
);'''

schema_photo_tag = '''create table if not exists photo_tag (
    photo_id integer references photos(id),
    tag_id integer references tags(id),
    confidence float
);'''


class FrequencyLock:
    def __init__(self, interval):
        self._lock = asyncio.Lock()
        self._interval = interval
        self._shutdown = False
        self._task = None

    async def acquire(self):
        await self._lock.acquire()

    async def run(self):
        self._task = asyncio.Task.current_task()
        while not self._shutdown:
            await asyncio.sleep(self._interval)
            if self._lock.locked():
                self._lock.release()

    async def stop(self):
        self._shutdown = True
        if self._task:
            await self._task
            self._task = None


class Tagger:
    def __init__(self, topdir=None, auth=None, database=None,
                 limit=None, interval=None, tasks=1):
        self.topdir = topdir
        self.auth = auth
        self.limit = limit
        self.interval = interval
        self.dbpath = database
        self.tasks = tasks

        self.init_db()

    def init_db(self):
        self.db = sqlite3.connect(self.dbpath)
        self.curs = self.db.cursor()
        self.create_tables()

    def create_tables(self):
        for schema in [schema_photos, schema_tags, schema_photo_tag]:
            self.curs.execute(schema)

    def image_is_in_database(self, image):
        res = self.curs.execute('select 1 from photos where path = ?',
                                (image,))
        return True if res.fetchone() else False

    async def find_images(self):
        for root, dirs, files in os.walk(self.topdir):
            for item in files:
                imgpath = Path(root) / item
                if imgpath.suffix.lower() not in ('.jpg', '.png'):
                    continue

                if self.image_is_in_database(str(imgpath)):
                    LOG.info('skipping %s (already in database)', imgpath)
                    continue

                with imgpath.open('rb') as imgfd:
                    try:
                        img = Image.open(imgfd)
                    except Exception as err:
                        LOG.warning('failed to open %s: %s', imgpath, err)
                        continue
                    finally:
                        img.close()

                yield imgpath

    async def tag(self, image):
        LOG.debug('starting tag task for %s', image)
        with aiohttp.MultipartWriter('form-data') as root:
            with image.open('rb') as fd:
                part = root.append(fd)
                part.set_content_disposition('form-data', name='image')

                await self.limiter.acquire()
                LOG.info('fetching data for %s', image)
                async with self.session.post(
                        'https://api.imagga.com/v2/tags', data=root) as res:
                    data = await res.json()

        return (image, data)

    def store(self, job):
        image, data = job

        if 'tags' not in data.get('result', {}):
            LOG.warning('no tags for %s', image)
            return image

        LOG.info('storing data for %s', image)
        self.curs.execute('insert into photos (path) '
                          'values (?)', (str(image),))

        photo_id = self.curs.lastrowid
        for tag in data['result']['tags']:
            tag_name = tag['tag']['en']
            tag_conf = tag['confidence']
            self.curs.execute('insert or ignore into tags (tag_name) '
                              'values (?)', (tag_name,))
            res = self.curs.execute('select id from tags '
                                    'where tag_name = ?', (tag_name,))
            tag_id = res.fetchone()[0]
            self.curs.execute('insert into photo_tag '
                              '(photo_id, tag_id, confidence) '
                              'values (?, ?, ?)', (photo_id, tag_id, tag_conf))

        self.db.commit()
        return image

    async def run(self):
        self.session = aiohttp.ClientSession(auth=self.auth)
        self.limiter = FrequencyLock(self.interval)
        asyncio.create_task(self.limiter.run())

        pl = stream.iterate(self.find_images())

        if self.limit:
            pl = pl | pipe.take(self.limit)

        pl = (
            pl |
            pipe.map(self.tag, task_limit=self.tasks) |
            pipe.map(self.store)
        )

        try:
            await pl
        finally:
            await self.limiter.stop()
            await self.session.close()


@click.command()
@click.option('-v', 'loglevel', count=True)
@click.option('--api-key')
@click.option('--api-secret')
@click.option('-l', '--limit', type=int)
@click.option('-i', '--interval', type=int, default=1)
@click.option('-c', '--credentials', type=click.File())
@click.option('-d', '--database', default='photos.sqlite')
@click.option('-t', '--tasks', type=int)
@click.argument('topdir')
def main(loglevel, api_key, api_secret, credentials, database, limit,
         interval, tasks, topdir):
    loglevel = ['WARNING', 'INFO', 'DEBUG'][min(loglevel, 2)]
    logging.basicConfig(level=loglevel)

    if credentials:
        c = json.load(credentials)
        api_key = c['api_key']
        api_secret = c['api_secret']

    if api_key is None or api_secret is None:
        raise click.ClickException('missing api credentials')

    auth = aiohttp.BasicAuth(login=api_key, password=api_secret)
    tagger = Tagger(
        topdir=topdir,
        auth=auth,
        database=database,
        interval=interval,
        limit=limit,
        tasks=tasks)

    asyncio.run(tagger.run())


if __name__ == '__main__':
    main()
