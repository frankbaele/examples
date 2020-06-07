import Database from 'better-sqlite3';

import axios from 'axios';
import parse from 'csv-parse';

import { v4 as uuidv4 } from 'uuid';

const openFoodFactsUrl = 'https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv';
const parser = parse({delimiter: '\t', columns: true, relax: true});



(async () => {
    // Create our sqlite database in memory
    const db = new Database('openFoodFacts.db');
    db.prepare('DROP TABLE IF EXISTS trade_items_open_food_facts').run();
    db.prepare('CREATE TABLE trade_items_open_food_facts (id TEXT PRIMARY KEY, gtin TEXT, name TEXT, brand text, nutriscore TEXT);').run();
    const insert = db.prepare('INSERT INTO trade_items_open_food_facts (id, gtin, name, brand, nutriscore) VALUES (@id, @gtin, @name, @brand, @nutriscore)');

    const response = await axios({
        method: 'get',
        url: openFoodFactsUrl,
        responseType: 'stream'
    });

    const stream = response.data.pipe(parser);
    console.log('starting import');

    function parseItem(item){
        insert.run({
            id: uuidv4(),
            gtin: item.code,
            name:item.product_name,
            brand: item.brands,
            nutriscore: item.nutriscore_grade && item.nutriscore_grade.toUpperCase ? item.nutriscore_grade.toUpperCase() : undefined,
        })
    }
    await new Promise((resolve, reject) => {
        stream
            .on('data', function(item) {
                parseItem(item)
            })
            .on('end', () => {
                resolve('End of the csv file');
            })
            .on('error', e => {
                reject(e)
            });
    })
    console.log("import is done")
    console.log("Count by nutriscore")
    const count = db.prepare("SELECT count(id), nutriscore from trade_items_open_food_facts group by nutriscore").all();
    console.log(count);
    console.log("Group by brand")
    const grouped = db.prepare("SELECT brand, count(id), group_concat(gtin) as gtin from trade_items_open_food_facts group by brand").all();
    console.log(grouped);
})();