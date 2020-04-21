import { default as pgp } from 'pg-promise';
import _ from 'lodash';
import moment from 'moment-timezone';
import * as JSONStream from 'JSONStream';
import QueryStream from 'pg-query-stream';

const ops = {
    insert: 'insert',
    update: 'update',
    query: 'query',
    delete: 'delete'
}

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

            let value = payload[key];
            if (value === Infinity) {
                value = 'infinity';
            }
            result.column_vals.push(payload[key] === '' ? null : value);
            result.column_names.push(`"${key}"`);
            result.bind_vars.push(`$${++i}`);
            return result;
        }, {
                column_vals: [],
                column_names: [],
                bind_vars: []
            });
    }

    async insert(payload, transform = true, allowPkInsert = false) {

        if (payload.id != undefined && !allowPkInsert) delete payload.id

        if (payload.createdAt != undefined) delete payload.createdAt;

        if (this.inboundPayloadConverter && transform)
            payload = this.inboundPayloadConverter(payload, ops.insert);

        let {column_vals, column_names, bind_vars} = this._deconstructPayload(payload);
        let insert = `insert into ${this.tableName} (${this._reduceToSeparatedString(column_names)})
                      values (${this._reduceToSeparatedString(bind_vars)}) returning ${this.fieldToSelect}`;

        var result = await this.conn.one(insert, column_vals);

        if (!transform) {
            return result;
        }
        return (this.outboundPayloadConverter) ? this.outboundPayloadConverter(result, ops.insert) : result;

    }

    _parseId(id) {

        if (!(typeof id == 'object'))
            return { name: "id", value: id };

        let keys = Object.keys(id);

        if (keys.length > 1 && keys.includes("id")){
           return { name: "id", value: id["id"] };
        }

        if (keys.length > 1 || keys.length == 0)
            throw "invalid id";

        return {
            name: `"${keys[0]}"`,
            value: id[keys[0]]
        };

    }

    async delete(idOrObject){
        const id = this._parseId(idOrObject);
        let deleteSql = `delete from ${this.tableName} where ${id.name} = $1`;
        await this.conn.none(deleteSql, [id.value]);
    }

    async update(id, payload, transform = true) {

        id = this._parseId(id);

        if (this.inboundPayloadConverter && transform)
            payload = this.inboundPayloadConverter(payload, ops.update);

        let {column_vals, column_names, bind_vars} = this._deconstructPayload(payload);
        let setString = column_names.reduce((prev, curr, i) => {

            if (!prev) return `${curr} = ${bind_vars[i]}`;
            return `${prev}, ${curr} = ${bind_vars[i]}`;
        }, null);

        column_vals.push(id.value);

        let update = `update ${this.tableName} set ${setString} where ${id.name} = $${column_vals.length} returning ${this.fieldToSelect}`;

        let result = await this.conn.one(update, column_vals);

        if (!transform) {
            return result;
        }
        return (this.outboundPayloadConverter) ? this.outboundPayloadConverter(result, ops.update) : result;

    }

    async byId(id, transform = true) {
        return await this.one({ id: id }, transform);
    }

    async all(page, transform = true) {
        let sql = `select ${this.fieldToSelect} from ${this.tableName}`;

        if (page && page.size && page.no) {
            sql += ` offset ${(page.size * page.no) - page.size} limit ${page.size}`;
        }
        let result = await this.conn.manyOrNone(sql);

        if (!transform) {
            return result;
        }

        return (this.outboundPayloadConverter) ? result.map(r => this.outboundPayloadConverter(r, ops.query)) : result;
    }

    async buildQuery(filter, page) {

        let {column_vals, column_names} = this._deconstructPayload(filter);

        let select = `select ${this.fieldToSelect} from ${this.tableName}`;
        if (!filter || Object.keys(filter).length == 0)
            return { select, column_vals };

        let columnValsIdsToBeRemoved = [];
        let bind_vars = [];
        let whereString = column_names.reduce((prev, curr, i) => {

            if (column_vals[i] === null) {
                columnValsIdsToBeRemoved.push(i);
                if (!prev) return `${curr} is null`;
                return `${prev} and ${curr} is null`;
            }
            const bind_var = '$' + (bind_vars.length + 1);
            bind_vars.push(bind_var);
            if (!prev) return `${curr} = ${bind_var}`;
            return `${prev} and ${curr} = ${bind_var}`;
        }, null);

        if (columnValsIdsToBeRemoved.length > 0) {
            column_vals = column_vals.filter((v, i) => columnValsIdsToBeRemoved.filter(ci => ci == i).length == 0);
        }

        select +=  ` where ${whereString}`;

        if (page && page.size && page.no) {
            select += ` offset ${(page.size * page.no) - page.size} limit ${page.size}`;
        }

        return { select, column_vals };
    }

    async query(filter, page, transform = true) {

        if (!filter || Object.keys(filter).length == 0)
            return this.all(page, transform);

        if (this.queryConverter && transform) {
            filter = this.queryConverter(filter);
        }

        let { select, column_vals } = await this.buildQuery(filter, page);
        let result = await this.conn.manyOrNone(select, column_vals);

        if (!transform) {
            return result;
        }

        return (this.outboundPayloadConverter) ? result.map(r => this.outboundPayloadConverter(r, ops.query)) : result;

    }

    async rawWhere(whereString, vals, transform = true) {

        let select = `select ${this.fieldToSelect} from ${this.tableName} where ${whereString}`;
        let result = await this.conn.manyOrNone(select, vals);

        if (!transform) {
            return result;
        }

        return (this.outboundPayloadConverter) ? result.map(r => this.outboundPayloadConverter(r, ops.query)) : result;
    }

    async rawWhereOne(whereString, vals, transform = true) {

        let select = `select ${this.fieldToSelect} from ${this.tableName} where ${whereString}`;
        let result = await this.conn.oneOrNone(select, vals);

        if (!transform) {
            return result;
        }

        return (this.outboundPayloadConverter) ? this.outboundPayloadConverter(result, ops.query) : result;
    }


    async many(filter, page, transform = true) {
        return await this.query(filter, page, transform);
    }

    async one(filter, transform = true) {
        let result = await this.query(filter, null, transform);
        return result[0];
    }

    multiQuery(queries, transform = true) {

        return Promise.all(queries.map(q => this.query(q, null, transform)))
            .then(result => _.flatten(result));

    }
}

export class ModifiedAtDataTable extends DbTable {

    constructor(tableName, connection) {
        super(tableName, connection)
    }

    inboundPayloadConverter(payload, op) {
        if (op == ops.update) {
            payload.modifiedAt = moment.tz().utc().format();
        }
        return payload;
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

        return await this.connection.tx(async (t) => {
            let ctx = new DbContext(t, this.dbTables, this.tableFactory);
            return await func(ctx);
        });
    }

    async streamJSON(query, parameters, responseStream) {
        const qs = new QueryStream(query, parameters);
        return this.connection.stream(qs, s => {
            s.pipe(JSONStream.stringify()).pipe(responseStream);
        });
    }
}
