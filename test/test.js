import test from 'ava';
import pg from 'pg-promise';
import fs from 'fs';
import {DbTable} from '../src/index';

const conn = pg()(process.env.DATABASE_URL || require('../config.json')["test-db"]);

test.before(async (t)=>{

   const seedSql = fs.readFileSync(__dirname + '/initDb.sql', "utf8");
   await conn.none(seedSql);
});

test('should be able to retrieve all records', async (t) => {
    const test = new DbTable('test',conn);

    var result = await test.query(undefined);
    t.is(result.length, 3);
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

test('should be able to infinity values into timestamps', async (t) => {
    const test = new DbTable('timestamps',conn);
    var result = await test.insert({id:10,validFrom: Infinity,validTo: Infinity}, true, true);    
});

test('should be able to delete by id in object', async (t) => {
    const test = new DbTable('timestamps',conn);
    await test.delete({id:1, test:3});    
});


test('should be able to delete by id', async (t) => {
    const test = new DbTable('timestamps',conn);
    await test.delete(2);    
});