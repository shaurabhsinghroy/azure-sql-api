var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;

const executeSQL = (context, verb, entity, payload) => {
    let result = [];    
    const paramPayload = (payload != null) ? JSON.stringify(payload) : '';
    context.log(`Executing ${verb}_${entity} with payload: ${paramPayload}`);

    const connection = new Connection({
        server: process.env["db_server"],
        authentication: {
            type: 'default',
            options: {
                userName: process.env["db_user"],
                password: process.env["db_password"],
            }
        },
        options: {
            database: process.env["db_database"],
            encrypt: true
        }
    });

    const request = new Request(`web.${verb}_${entity}`, (err) => {
        if (err) {
            context.log.error(err);            
            context.res = {
                status: 500,
                body: "Error executing T-SQL command"
            };
        } else {
            context.res = {
                status: 200,
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(result.length > 0 ? result : { message: "Operation successful" })
            };   
        }
        context.done();
    });    

    if (payload) {
        request.addParameter('Json', TYPES.NVarChar, paramPayload, Infinity);
    }

    request.on('row', columns => {
        let rowObject = {};
        columns.forEach(column => {
            rowObject[column.metadata.colName] = column.value;
        });
        result.push(rowObject);
    });

    connection.on('connect', err => {
        if (err) {
            context.log.error(err);              
            context.res = {
                status: 500,
                body: "Error connecting to Azure SQL query"
            };
            context.done();
        } else {
            connection.callProcedure(request);
        }
    });

    connection.connect();
}

module.exports = function (context, req) {    
    const method = req.method.toLowerCase();
    let payload = null;
    let entity = "";

    switch(method) {
        case "get":
            if (req.params.id) {
                entity = "customer";
                payload = { "CustomerID": req.params.id };            
            } else {
                entity = "customers";                
            }
            break;
        case "put":  // INSERT/UPDATE
        case "patch": // Partial update
            entity = "customer";
            payload = req.body;  
            if (req.params.id) 
                payload.CustomerID = req.params.id;
            break;
        case "delete":
            entity = "customer";
            if (req.params.id) {
                payload = { "CustomerID": req.params.id };            
            }
            break;       
    }

    executeSQL(context, method, entity, payload);
}
