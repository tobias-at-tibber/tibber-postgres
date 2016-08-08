import test from 'ava';
import pg from 'pg-promise';
import fs from 'fs';
import {DbTable} from '../src/index';

const conn = pg()(process.env.DATABASE_URL || require('../config.json')["test-db"]);

test.before(async (t)=>{

   const seedSql = fs.readFileSync(__dirname + '/initDb.sql', "utf8");
   await conn.none(seedSql);
});

test('should be able to run queries with null values', async (t) => {
    const test = new DbTable('test',conn);

    var result = await test.query({stringCol:null});
    t.is(result.length, 1);
});

test('should be able to combine null and not null query values', async (t) => {
    const test = new DbTable('test',conn);

    var result = await test.query({stringCol:null, integerCol : 1});
    t.is(result.length, 1);
});