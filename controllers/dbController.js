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
const limitCount = " LIMIT 1000";
const lockCheck = " FOR UPDATE";

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

function selectNodeOnYear (year) {
    return new Promise ((resolve, reject) => {
        resolve (year < 1980 ? beforeConnection : afterConnection);
    });
}

function buildSelectQuery (body) {
        let nonEmptyParams;
        for (const key in body) {
            if (body.key !== '')
                nonEmptyParams++;
        }
        console.log ("Param count: " + nonEmptyParams);
        if (nonEmptyParams == 0)
            return (["SELECT * FROM movies", []]);
        else {
            const keys = Object.keys (body);
            let query = "SELECT * FROM movies WHERE ";
            let values = [];
            for (let i = 0; i < keys.length; i++) {
                if (keys[i] === 'id' && body.id.trim ().length !== 0) {
                    query += "id = ?";
                    values.push (body.id.trim ());
                    if (i !== keys.length - 1)
                        query += " AND ";
                }
                else if (keys[i] === 'name' && body.name.trim ().length !== 0) {
                    query += "name LIKE ?";
                    values.push ("%" + body.name + "%");
                    if (i !== keys.length - 1)
                        query += " AND ";
                }
                else if (keys[i] === 'year' && body.year.trim ().length !== 0) {
                    query += "year = ?";
                    values.push (parseInt (body.year.trim ()));
                    if (i !== keys.length - 1)
                        query += " AND ";
                }
                else if (keys[i] === 'rank' && body.rank.trim ().length !== 0) {
                    query += "rank = ?";
                    values.push (parseFloat (body.rank.trim ()));
                    if (i !== keys.length - 1)
                        query += " AND ";
                }
            }

            return ([query, values]);
        }   
}

function buildDeleteQuery (body) {

}

function setIsolationLevel (connection, level) {
    return new Promise ((resolve, reject) => {
        if (connection === 'main') {
            mainConnection.query ('SET TRANSACTION ISOLATION LEVEL ' + level, (err) => {
                if (err) {
                    reject (err);
                }
                else {
                    resolve (level);
                }
            });
        }
        else if (connection === 'before') {
            beforeConnection.query ('SET TRANSACTION ISOLATION LEVEL ' + level, (err) => {
                if (err) {
                    reject (err);
                }
                else {
                    resolve (level);
                }
            });
        }
        else if (connection === 'after') {
            afterConnection.query ('SET TRANSACTION ISOLATION LEVEL ' + level, (err) => {
                if (err) {
                    reject (err);
                }
                else {
                    resolve (level);
                }
            });
        }
        else {
            console.log ("unkown connection");
        }
    });
}


const dbController = {
    select: async (req, res) => {
        req.body = {        //test req body
            year: '2002',
            name: '18 and '
        };

        const [query, values] = buildSelectQuery (req.body);
        let useBackup = false;
        awaitAllConnections () .then (results => {
            if (results[0].status !== 'rejected') {     //main node is down
                setIsolationLevel ('main', "REPEATABLE READ").then (result => {
                    mainConnection.query (query + lockCheck, values, (err, results) => {
                        console.log (results);
                        if (results.length === 0) {
                            mainConnection.beginTransaction (err => {
                                if (err) throw err;
        
                                mainConnection.query (query + limitCount, values, (err, results) => {
                                    console.log (query);
                                    console.log (values);
                                    console.log (results);
                                    console.log (useBackup);
                                    if (err) {
                                        mainConnection.rollback ();
                                        console.log (err);
                                        console.log ('Select transaction was rolled back2');
                                        mainConnection.close ();
                                    }
        
                                    mainConnection.commit (err => {
                                        if (err) {
                                            mainConnection.rollback ();
                                            console.log (err);
                                            console.log ('Select transaction was rolled back3');
                                            mainConnection.close ();
                                        }
                                    });
        
                                    console.log ("Select query successful");
                                    mainConnection.close ();
                                });
                            });
                        }
                        else
                            useBackup = true;
                    });
                    
                });
            }
            if (results[0].status === 'rejected' || useBackup) {      //use backup nodes
                selectNodeOnYear (parseInt (req.body.year)).then (connection => {
                    setIsolationLevel (parseInt (req.body.year) < 1980 ? 'before' : 'after', "REPEATABLE READ").then (results => {
                        connection.beginTransaction (err => {
                            if (err) throw err;
    
                            connection.query (query, values, (err, results) => {
                                console.log (query);
                                console.log (values);
                                console.log (results);
                                console.log ("Backup " + useBackup);
                                if (err) {
                                    connection.rollback ();
                                    console.log (err);
                                    console.log ('Select transaction was rolled back2');
                                    connection.close ();
                                }
    
                                connection.commit (err => {
                                    if (err) {
                                        connection.rollback ();
                                        console.log (err);
                                        console.log ('Select transaction was rolled back3');
                                        connection.close ();
                                    }
                                });
    
                                console.log ("Select query successful");
                                connection.close ();
                            });
                        });
                    });
                });
            }
        });

        res.render ('index2');
    },

    insert: async (req, res) => {
        awaitAllConnections ().then (results => {

        });
    },

    update: (req, res) => {

    },

    delete: (req, res) => {
        
    }
};

module.exports = dbController;