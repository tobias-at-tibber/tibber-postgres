import { default as pgp } from 'pg-promise';
import _ from 'lodash';

export class DbTable {

    constructor(tableName, conn, fieldToSelect) {
        this.tableName = tableName;
        this.conn = conn;
        this.fieldToSelect = fieldToSelect || '*';
    }

    _reduceToSeparatedString(arr, separator = ",") {
        return arr.reduce((prev, curr) => {

            if (!prev)
                return curr;

            return `${prev}${separator}${curr}`;

        }, null);
    }

    _deconstructPayload(payload) {

        return Object.keys(payload).reduce((result, key, i) => {
            result.column_vals.push(payload[key]);
            result.column_names.push(`"${key}"`);
            result.bind_vars.push(`$${++i}`);
            return result;
        }, {
                column_vals: [],
                column_names: [],
                bind_vars: []
            });
    }



    insert(payload) {

        if (payload.id) delete payload.id

        if (this.inboundPayloadConverter)
            payload = this.inboundPayloadConverter(payload);

        let {column_vals, column_names, bind_vars} = this._deconstructPayload(payload);
        let insert = `insert into ${this.tableName} (${this._reduceToSeparatedString(column_names)}) 
                      values (${this._reduceToSeparatedString(bind_vars)}) returning ${this.fieldToSelect}`;
                      
        

        return this.conn.one(insert, column_vals);
    }

    _parseId(id) {

        if (!(typeof id == 'object'))
            return { name: "id", value: id };

        let keys = Object.keys(id);

        if (keys.length > 1 || keys.length == 0)
            throw "invalid id";

        return {
            name: `"${keys[0]}"`,
            value: id[keys[0]]
        };

    }

    async update(id, payload) {

        id = this._parseId(id);

        if (this.inboundPayloadConverter)
            payload = this.inboundPayloadConverter(payload);

        let {column_vals, column_names, bind_vars} = this._deconstructPayload(payload);
        let setString = column_names.reduce((prev, curr, i) => {

            if (!prev) return `${curr} = ${bind_vars[i]}`;
            return `${prev}, ${curr} = ${bind_vars[i]}`;
        }, null);

        column_vals.push(id.value);

        let update = `update ${this.tableName} set ${setString} where ${id.name} = $${column_vals.length} returning ${this.fieldToSelect}`;
        
        let result = await this.conn.one(update, column_vals);

        return (this.outboundPayloadConverter) ? this.outboundPayloadConverter(result) : result;

    }

    async byId(id) {
        return await this.one({ id: id });
    }

    async all(page) {
        let sql = `select ${this.fieldToSelect} from ${this.tableName}`;

        if (page && page.size && page.no) {
            sql += ` offset ${(page.size * page.no) - page.size} limit ${page.size}`;
        }
        let result = await this.conn.manyOrNone(sql);
        

        return (this.outboundPayloadConverter) ? result.map(this.outboundPayloadConverter) : result;
    }

    async query(filter, page) {

        if (!filter || Object.keys(filter).length == 0)
            return this.all(page);

        if (this.queryConverter) {
            filter = this.queryConverter(filter);
        }
        let {column_vals, column_names, bind_vars} = this._deconstructPayload(filter);
        let whereString = column_names.reduce((prev, curr, i) => {
            if (!prev) return `${curr} = ${bind_vars[i]}`;
            return `${prev} and ${curr} = ${bind_vars[i]}`;
        }, null);

        let select = `select ${this.fieldToSelect} from ${this.tableName} where ${whereString}`;

        if (page && page.size && page.no) {
            select += ` offset ${(page.size * page.no) - page.size} limit ${page.size}`;
        }
        let result = await this.conn.manyOrNone(select, column_vals);

        return (this.outboundPayloadConverter) ? result.map(this.outboundPayloadConverter) : result;

    }

    async rawWhere(whereString, vals) {

        let select = `select ${this.fieldToSelect} from ${this.tableName} where ${whereString}`;
        let result = await this.conn.manyOrNone(select, vals);
        return (this.outboundPayloadConverter) ? result.map(this.outboundPayloadConverter) : result;
    }

    async rawWhereOne(whereString, vals) {

        let select = `select ${this.fieldToSelect} from ${this.tableName} where ${whereString}`;
        let result = await this.conn.oneOrNone(select, vals);
        return (this.outboundPayloadConverter) ? this.outboundPayloadConverter(result) : result;
    }


    async many(filter) {
        return await this.query(filter);
    }

    async one(filter) {
        let result = await this.query(filter);
        return result[0];
    }

    multiQuery(queries) {

        return Promise.all(queries.map(q => this.query(q)))
            .then(result => _.flatten(result));

    }    
}

export class DbTableFactory {

    create(tableName, connection) {
        return new DbTable(tableName, connection);
    }
}



export class DbContext {

    constructor(connection, dbtables, tableFactory) {

        this.dbTables = dbtables;
        this.connection = connection;
        this.tableFactory = tableFactory || new DbTableFactory();

        dbtables.forEach(t => {
            this[t.refName] = this.tableFactory.create(t.tableName, this.connection);
        });
    }

    async inTransaction(func) {

        await this.connection.tx(async (t) => {
            var ctx = new DbContext(t, this.dbTables, this.tableFactory);
            await func(ctx);
        });
    }

}


