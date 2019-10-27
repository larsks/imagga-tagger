import json
import sqlite3
import sys


srcdb = sqlite3.connect(sys.argv[1])
srccurs = srcdb.cursor()

destdb = sqlite3.connect(sys.argv[2])
destcurs = destdb.cursor()

srccurs.execute('select id, path from photos')
for photo in srccurs:
    print('processing', photo[1])
    tagcurs = srcdb.cursor()
    tagcurs.execute('select tag_name, confidence from '
                    'tags join photo_tag '
                    'on (tags.id = photo_tag.tag_id) '
                    'where photo_tag.photo_id = ?',
                    (photo[0],))

    tags = tagcurs.fetchall()
    data = {
        'path': photo[1],
        'tags': dict(tags),
    }

    destcurs.execute('insert or ignore into photos (path, data) values (?, ?)',
                     (data['path'], json.dumps(data)))

destdb.commit()
