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


class Tagger:
    def __init__(self, topdir=None, auth=None, database=None):
        self.topdir = topdir
        self.auth = auth
        self.db = sqlite3.connect(database)
        self.curs = self.db.cursor()

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
        LOG.info('fetching data for %s', image)
        with aiohttp.MultipartWriter('form-data') as root:
            with image.open('rb') as fd:
                part = root.append(fd)
                part.set_content_disposition('form-data', name='image')

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

    async def run(self, limit=None):
        self.session = aiohttp.ClientSession(auth=self.auth)

        pl = stream.iterate(self.find_images())

        if limit:
            pl = pl | pipe.take(limit)

        pl = (
            pl |
            pipe.spaceout(1) |
            pipe.map(self.tag, task_limit=1) |
            pipe.map(self.store)
        )

        try:
            await pl
        finally:
            await self.session.close()


@click.command()
@click.option('-v', 'loglevel', count=True)
@click.option('--api-key')
@click.option('--api-secret')
@click.option('-l', '--limit', type=int)
@click.option('-c', '--credentials', type=click.File())
@click.option('-d', '--database', default='photos.sqlite')
@click.argument('topdir')
def main(loglevel, api_key, api_secret, credentials, database, limit, topdir):
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
        database=database)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(tagger.run(limit=limit))
    loop.close()


if __name__ == '__main__':
    main()
