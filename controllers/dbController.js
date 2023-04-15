const mysql = require ('mysql2');

const connectionMain = {
    host: '34.126.65.162',  //34.126.65.162
    user: 'root',
    password: 'allmovies',
    database: 'all-movies'
}

const connectionBefore1980 = {
    host: '34.87.95.70',    //34.87.95.70
    user: 'root',
    password: 'before1980',
    database: 'before1980'
}

const connectionAfter1980 = {
    host: '35.185.182.140', //35.185.182.140
    user: 'root',
    password: 'after1980',
    database: 'movies1980'
}

const poolCluster = mysql.createPoolCluster ();
poolCluster.add ('MAIN', connectionMain);
poolCluster.add ('BEFORE1980', connectionBefore1980);
poolCluster.add ('AFTER1980', connectionAfter1980);

let mainConnection;
let beforeConnection;
let afterConnection;
let connectionToUse;

function acquireMainConnection () {
    return new Promise ((resolve, reject) => {
        poolCluster.getConnection ('MAIN', (err, connection) => {
            if (err) {
                console.log ('Failed to connect to main, proceeding to backup nodes.', err);
                reject ("mainFail");
            }
            else {
                console.log ('Connection to the main node successful');
                mainConnection = connection;
                resolve (connection);
            }
        });
    });
}

function acquireBeforeConnection () {
    return new Promise ((resolve, reject) => {
        poolCluster.getConnection ('BEFORE1980', (err, connection) => {
            if (err) {
                console.log ('Failed to connect to before-1980 node, proceeding to main.', err);
                reject ("beforeFail");
            }
            else {
                console.log ('Successfully connected to before-1980 node');
                beforeConnection = connection;
                resolve (connection);
            }
        });
    });
}

function acquireAfterConnection () {
    return new Promise ((resolve, reject) => {
        poolCluster.getConnection ('AFTER1980', (err, connection) => {
            if (err) {
                console.log ('Failed to connect to after-1980 node, proceed to main', err);
                reject ("afterFail");
            }
            else {
                console.log ('Successfully connected to after-1980 node');
                afterConnection = connection;
                resolve (connection);
            }
        })
    });
}

function awaitAllConnections () {
    return new Promise ((resolve, reject) => {
        Promise.allSettled ([acquireMainConnection (), acquireBeforeConnection (), acquireAfterConnection ()])
        .then (results => {
            resolve (results);
        })
        .catch (errs => {
            reject (errs);
        });
    })
}

const dbController = {
    select: async (req, res) => {
        awaitAllConnections () .then (results => {
            if (results[0].status !== 'rejected') {     //main node is down
                mainConnection.query ("SELECT * FROM movies WHERE id = ?", [412321], (err, result) => {
                    console.log (result);
                    mainConnection.release ();
                });
            }
            else {      //use backup nodes
                beforeConnection.query ("SELECT * FROM movies WHERE id = ?", [412321], (err, result) => {
                    console.log (result);
                    beforeConnection.release ();
                });

                afterConnection.query ("SELECT * FROM movies WHERE id = ?", [412321], (err, result) => {
                    console.log (result);
                    afterConnection.release ();
                });
            }
        });

        res.render ('index2');
    },

    insert: async (req, res) => {

    },

    update: (req, res) => {

    },

    delete: (req, res) => {
        
    }
};

module.exports = dbController;