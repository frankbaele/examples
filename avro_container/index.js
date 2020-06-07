import avsc from 'avsc';

const schema = {
    type: 'record',
    name: 'user',
    fields: [
        {name: 'first_name', type: 'string'},
        {name: 'last_name', type: 'string'},
        // By setting the default to null and allowing the null value
        // we can create an optional field in Avro
        {name: 'middle_name', default: null, type: ['null', 'string']},
        // Array with nested objects
        {
            name: 'children',
            default: null,
            type: [
                'null',
                {
                    type: 'array',
                    items: [
                        {
                            type: 'record',
                            name: 'child',
                            fields: [
                                {name: 'first_name', type: 'string'}
                            ],
                        },
                    ],
                },
            ],
        },
    ]
}
const type = avsc.Type.forSchema(schema);

const encoder = avsc.createFileEncoder('./users.avro', schema);

function write(item) {
    return new Promise((resolve, reject) => {
        const result = type.isValid(item, {
            errorHook: err => {
                err.forEach(key => {
                    console.log('Invalid field name: ' + key);
                    console.log('Invalid field content: ' + JSON.stringify(item[key], null, 2));
                });
                reject();
            },
        });
        if (result) {
            encoder.write(item, resolve);
        }
    });
}

write({
    "first_name": "Frank",
    "last_name": "Baele",
    "children": [{
        "first_name": "Bort"
    }]
})

write({
    "first_name": "Peter",
    "last_name": "Peterson",
    "children": [{
        "first_name": "Jef"
    }]
})

encoder.end();

// Reading part
import fs from 'fs';

const src = fs.createReadStream('./users.avro');
src.pipe(new avsc.streams.BlockDecoder())
    .on('data', function (item) {
        console.log(item);
    })
    .on('end', () => {
        console.log('All records read')
    });

// Snappy example

import snappy from 'snappy';
import crc32 from 'buffer-crc32';

const snappyEncoder = avsc.createFileEncoder('./snappy.avro', schema, {
    snappy: function (buf, cb) {
        const checksum = crc32(buf);
        snappy.compress(buf, function (err, deflated) {
            if (err) {
                cb(err);
                return;
            }
            const block = Buffer.alloc(deflated.length + 4);
            deflated.copy(block);
            checksum.copy(block, deflated.length);
            cb(null, block);
        });
    }
})

snappyEncoder.write({
    "first_name": "Frank",
    "last_name": "Baele",
    "children": [{
        "first_name": "Bort"
    }]
});

snappyEncoder.end();

avsc.createFileDecoder('./snappy.avro', {
    snappy: function (buf, cb) {
        // Avro appends checksums to compressed blocks, which we skip here.
        return snappy.uncompress(buf.slice(0, buf.length - 4), cb);
    }
})
    .on('metadata', function (type) {
        console.log(type)
    })
    .on('data', function (val) {
        console.log(val)
    });

// Gzip example