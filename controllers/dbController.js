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
const isolationLevel = "REPEATABLE READ"

const limitCount = " LIMIT 50";
const lockCheck = " FOR UPDATE";
let nodeQueue = [];
let mainQueue = [];

function acquireMainConnection () {
    return new Promise ((resolve, reject) => {
        poolCluster.getConnection ('MAIN', (err, connection) => {
            console.log ("im trying");
            if (err) {
                console.log ('Failed to connect to main, proceeding to backup nodes.', err);
                reject (err);
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
        if (typeof mainConnection !== 'undefined') {
            mainConnection.release ();
            mainConnection.destroy ();
        }
        if (typeof beforeConnection !== 'undefined') {
            beforeConnection.release ();
            afterConnection.release ();
            beforeConnection.destroy ();
            afterConnection.destroy ();
        }
        
        Promise.allSettled ([acquireMainConnection (), acquireBeforeConnection (), acquireAfterConnection ()])
        .then (results => {
            resolve (results);
        })
        .catch (errs => {
            reject (errs);
        });
    })
}

async function getNewestId () {
    return new Promise ((resolve, reject) => {
        let ids = [];
        const queryPromise = (connection) => {
            return new Promise((resolve, reject) => {
                connection.query("SELECT MAX(id) AS last_id FROM movies", (err, result) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(result[0].last_id);
                    }
                });
            });
        };

        const mainQuery = typeof mainConnection !== 'undefined' ? queryPromise(mainConnection) : Promise.resolve(null);
        const beforeQuery = typeof beforeConnection !== 'undefined' ? queryPromise(beforeConnection) : Promise.resolve(null);
        const afterQuery = typeof afterConnection !== 'undefined' ? queryPromise(afterConnection) : Promise.resolve(null);

        Promise.all([mainQuery, beforeQuery, afterQuery])
        .then(results => {
            ids = results.filter(id => id !== null);
            resolve (Math.max(...ids) + 1);
        })
        .catch(err => {
            reject(err);
        });
    });
    
}

function buildLockQuery (id) {
    return ["SELECT * FROM movies WHERE id = ? " + lockCheck, [parseInt (id)]];
}
function buildSelectQuery (body) {
    let nonEmptyParams = 0;
    for (const key in body) {
        if (body[key] !== '')
            nonEmptyParams++;
    }
    console.log ("Param count: " + nonEmptyParams);
    if (nonEmptyParams == 0)
        return (["SELECT * FROM movies", []]);
    else {
        const keys = Object.keys (body).filter (key => body[key] !== '');
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
                query += "movie_rank = ?";
                values.push (parseFloat (body.rank.trim ()));
                if (i !== keys.length - 1)
                    query += " AND ";
            }
        }

        return ([query, values]);
    }
}

function buildInsertQuery (body) {
    const keys = Object.keys (body);
    let values = [];
    let query = "INSERT INTO movies (id, name, year, movie_rank) VALUES (?, ?, ?, ?)"

    return new Promise ((resolve, reject) => {
        getNewestId ().then (result => {
            values.push (result);
            values.push (body.name.trim ())
            values.push (parseInt (body.year.trim ()));
            values.push (parseFloat (body.rank.trim ()));
            resolve ([query, values]);
        });
    });

    // for (let i = 0; i < keys.length; i++) {          //tinamad na ako iautomate hehe
    //     if (keys[i] === 'name' && body.name.trim ().length !== 0) {
    //         query += "name = ?";
    //         values.push (body.name);
    //         if (i !== keys.length - 1)
    //             query += ", ";
    //     }
    //     else if (keys[i] === 'year' && body.year.trim ().length !== 0) {
    //         query += "year = ?";
    //         values.push (parseInt (body.year.trim ()));
    //         if (i !== keys.length - 1)
    //             query += ", ";
    //     }
    //     else if (keys[i] === 'rank' && body.rank.trim ().length !== 0) {
    //         query += "movie_rank = ?";
    //         values.push (parseFloat (body.rank.trim ()));
    //         if (i !== keys.length - 1)
    //             query += ", ";
    //     }
    // }
}

function buildUpdateQuery (body) {
    const keys = Object.keys (body);
    let query = "UPDATE movies SET ";
    let values = [];

    for (let i = 0; i < keys.length; i++) {
        if (keys[i] === 'name' && body.name.trim ().length !== 0) {
            query += "name = ?";
            values.push (body.name);
            if (i !== keys.length - 1)
                query += ", ";
        }
        else if (keys[i] === 'year' && body.year.trim ().length !== 0) {
            query += "year = ?";
            values.push (parseInt (body.year.trim ()));
            if (i !== keys.length - 1)
                query += ", ";
        }
        else if (keys[i] === 'rank' && body.rank.trim ().length !== 0) {
            query += "movie_rank = ?";
            values.push (parseFloat (body.rank.trim ()));
            if (i !== keys.length - 1)
                query += ", ";
        }
    }

    query += " WHERE id = ?";
    values.push (parseInt (body.id));

    return ([query, values]);
}

function buildDeleteQuery (id) {
    const query = "DELETE FROM movies WHERE id = ?";
    const values = [parseInt (id)];
    return [query, values];
}

function performQuery (connection, connectionName, lockQuery, lockValues, query, values) {
    return new Promise ((resolve, reject) => {
        setIsolationLevel (connectionName, isolationLevel).then (result => {
            //NEED OWN SELECT FOR UPDATE QUERY STRING
            connection.query (lockQuery, lockValues, (lockErr, lockResult) => {
                console.log ();
                console.log ("lockErr: " + lockErr);
                console.log (lockResult.length);
                const queryType = query.split(" ")[0].toLowerCase ();
                if ((lockResult.length !== 0 && queryType !== 'insert') ||
                    (lockResult.length === 0 && (queryType === 'insert' || queryType === 'select')) ) {
                    connection.beginTransaction (transErr => {
                        if (transErr) reject (transErr);
    
                        connection.query (query, values, (queryErr, results) => {
                            console.log (query);
                            console.log (values);
                            console.log (queryErr);
                            console.log (results);
                            if (queryErr) {
                                connection.rollback ();
                                connection.release ()
                                connection.destroy ();
                                console.log (queryType + ' transaction was rolled back1');
                                reject (queryErr);
                                return;
                            }
    
                            connection.commit (commitErr => {
                                if (commitErr) {
                                    connection.rollback ();
                                    connection.release ()
                                    connection.destroy ();
                                    console.log (queryType + ' transaction was rolled back2');
                                    reject (commitErr);
                                    return;
                                }
                            });
    
                            console.log (queryType + ' transaction to ' + connectionName + ' was successful');
                            connection.release ();
                            connection.destroy ();
                            resolve (results);
                            return;
                        });
                    });
                }
                else
                    reject ("locked");
            }); 
        });
    });
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
        const [query, values] = buildSelectQuery (req.body);
        let useBackup = false;
        awaitAllConnections () .then (results => {
            if (results[0].status !== 'rejected') {     //main node is down
                console.log (query);
                console.log (values);

                performQuery (mainConnection, 'main', query + limitCount + lockCheck, values, query + limitCount, values)
                .then ((results) => {
                    res.send (results);
                    useBackup = false;
                    return;
                })
                .catch (() => {
                    useBackup = true;
                });
            }

            if (results[0].status === 'rejected' || useBackup) {      //use backup nodes
                const before1980 = parseInt (req.body.year) < 1980
                performQuery (before1980 ? beforeConnection : afterConnection, before1980 ? 'before' : 'after', query + limitCount + lockCheck, values, query + limitCount, values).then ((results) => {
                    res.send (results);
                    useBackup = false;
                    return;
                })
            }
        });
    },

    insert: async (req, res) => {
        const before1980 = parseInt (req.body.year) < 1980;
        let useBackup = false;
        awaitAllConnections ().then (results => {
            buildInsertQuery (req.body).then ((buildResults) => {
                const [query, values] = buildResults;
                const [lockQuery, lockValues] = buildLockQuery (values[0]);

                if (results[0].status !== 'rejected') {     //main node is down
                    performQuery (mainConnection, 'main', lockQuery, lockValues, query, values)
                    .then (() => {
                        useBackup = false;
                        
                        nodeQueue.push ({
                            query: query,
                            values: values,
                            node: before1980 ? 'before' : 'after',
                            lockQuery: lockQuery,
                            lockValues: lockValues
                        });

                        console.log ('succeeded inserting to main: ');
                        console.log (query);
                        res.redirect ('/');
                    })
                    .catch (() => {
                        useBackup = true;
                    });
                }

                if (results[0].status === 'rejected' || useBackup) {      //use backup nodes
                    console.log ("using backup");
                    performQuery (before1980 ? beforeConnection : afterConnection, before1980 ? 'before' : 'after', lockQuery, lockValues, query, values).then (() => {
                        useBackup = false;

                        mainQueue.push ({
                            query: query,
                            values: values,
                            lockQuery: lockQuery,
                            lockValues: lockValues
                        });

                        
                        console.log ('succeeded inserting to' + (before1980) ? 'before' : 'after' + 'node: ');
                        console.log (query);
                        res.redirect ('/');
                    });
                }
            });
        });
    },

    update: async (req, res) => {
        let useBackup = false;

        console.log (req.body);
        const [query, values] = buildUpdateQuery (req.body);
        const [lockQuery, lockValues] = buildLockQuery (req.body.id);
        const before1980 = parseInt (req.body.year) < 1980;

        awaitAllConnections ().then (results => {
            if (results[0].status !== 'rejected') {     //main node is down
                performQuery (mainConnection, 'main', lockQuery, lockValues, query, values)
                .then (() => {
                    useBackup = false;
                    
                    nodeQueue.push ({
                        query: query,
                        values: values,
                        node: before1980 ? 'before' : 'after',
                        lockQuery: lockQuery,
                        lockValues: lockValues
                    });

                    res.redirect ('/');

                })
                .catch (() => {
                    useBackup = true;
                });
            }

            if (results[0].status === 'rejected' || useBackup) {      //use backup nodes
                performQuery (before1980 ? beforeConnection : afterConnection, before1980 ? 'before' : 'after', lockQuery, lockValues, query, values).then (() => {
                    useBackup = false;

                    mainQueue.push ({
                        query: query,
                        values: values,
                        lockQuery: lockQuery,
                        lockValues: lockValues
                    });

                    res.redirect ('/');
                })
            }
        });
    },

    delete: async (req, res) => {
        let useBackup = false;

        console.log (req.body);
        const [query, values] = buildDeleteQuery (req.body.id);
        const [lockQuery, lockValues] = buildLockQuery (req.body.id);
        const before1980 = parseInt (req.body.year) < 1980;
        awaitAllConnections () .then (results => {
            if (results[0].status !== 'rejected') {     //main node is down
                performQuery (mainConnection, 'main', lockQuery, lockValues, query, values)
                .then (() => {
                    useBackup = false;
                    
                    nodeQueue.push ({
                        query: query,
                        values: values,
                        node: before1980 ? 'before' : 'after',
                        lockQuery: lockQuery,
                        lockValues: lockValues
                    });
                })
                .catch (() => {
                    useBackup = true;
                });
            }

            if (results[0].status === 'rejected' || useBackup) {      //use backup nodes
                performQuery (before1980 ? beforeConnection : afterConnection, before1980 ? 'before' : 'after', lockQuery, lockValues, query, values).then (() => {
                    useBackup = false;

                    mainQueue.push ({
                        query: query,
                        values: values,
                        lockQuery: lockQuery,
                        lockValues: lockValues
                    });
                })
            }
        });

        res.redirect ('/');
    },

    replicate: async (req, res) => {
        res.send ("replic");
        return;
        // replication v3
        // while (nodeQueue.length !== 0) {
        //     const transaction = nodeQueue[0];
        //     if (typeof beforeConnection !== 'undefined') {
        //         const connection = (transaction.node === 'before') ? beforeConnection : afterConnection;
        //         console.log ("tracsaciont tp be repliacated to " + transaction.node + " node:");
        //         console.log (transaction);
        //         nodeQueue.shift ();
        //         performQuery (connection, transaction.node, transaction.lockQuery, transaction.lockValues, transaction.query, transaction.values)
        //         .then ((results) => {
        //             console.log ("succeeded in replicating to " + transaction.node + " node.");
        //         })
        //         .catch (() => {
        //             nodeQueue.unshift (transaction);
        //             res.send ("failed to replicate to " + transaction.node + " node.");
        //             return;
        //         });
        //     }
        //     else {
        //         Promise.allSettled ([acquireBeforeConnection (), acquireAfterConnection ()])
        //         .then (conResults => {
        //             const connection = (transaction.node === 'before') ? beforeConnection : afterConnection;
        //             console.log ("tracsaciont tp be repliacated to " + transaction.node + " node:");
        //             console.log (transaction);
        //             nodeQueue.shift ();
        //             performQuery (connection, transaction.node, transaction.lockQuery, transaction.lockValues, transaction.query, transaction.values)
        //             .then ((results) => {
        //                 console.log ("succeeded in replicating to " + transaction.node + " node.");
        //             })
        //             .catch (() => {
        //                 nodeQueue.unshift (transaction);
        //                 res.send ("failed to replicate to " + transaction.node + " node.");
        //                 return;
        //             });
        //         })
        //         .catch (() => {
        //             console.log ()
        //             res.send ("failed to connect to " + transaction.node + " node.");
        //             return;
        //         });

        //         if (typeof beforeConnection === 'undefined')
        //             break;
        //     }
        // }

        // while (mainQueue.length !== 0) {
        //     const transaction = mainQueue[0];
        //     if (typeof mainConnection !== 'undefined') {
        //         const connection = (transaction.node === 'before') ? beforeConnection : afterConnection;
        //         console.log ("tracsaciont tp be repliacated to " + transaction.node + " node:");
        //         console.log (transaction);
        //         nodeQueue.shift ();
        //         performQuery (connection, transaction.node, transaction.lockQuery, transaction.lockValues, transaction.query, transaction.values)
        //         .then ((results) => {
        //             console.log ("succeeded in replicating to " + transaction.node + " node.");
        //         })
        //         .catch (() => {
        //             nodeQueue.unshift (transaction);
        //             res.send ("failed to replicate to " + transaction.node + " node.");
        //             return;
        //         });
        //     }
        //     else {
        //         acquireMainConnection ()
        //         .then (conResults => {
        //             const connection = (transaction.node === 'before') ? beforeConnection : afterConnection;
        //             console.log ("tracsaciont tp be repliacated to " + transaction.node + " node:");
        //             console.log (transaction);
        //             nodeQueue.shift ();
        //             performQuery (connection, transaction.node, transaction.lockQuery, transaction.lockValues, transaction.query, transaction.values)
        //             .then ((results) => {
        //                 console.log ("succeeded in replicating to " + transaction.node + " node.");
        //             })
        //             .catch (() => {
        //                 nodeQueue.unshift (transaction);
        //                 res.send ("failed to replicate to " + transaction.node + " node.");
        //                 return;
        //             });
        //         })
        //         .catch ((error) => {
        //             console.log (error);
        //             // res.send ("failed to replicate to " + transaction.node + " node.");
        //             return;
        //         });

        //         if (typeof mainConnection === 'undefined')
        //             break;
        //     }
        // }

        //replication v2
        // awaitAllConnections () .then (results => {
        //     while (nodeQueue.length !== 0 && typeof beforeConnection !== 'undefined') {
        //         const transaction = nodeQueue[0];
        //         Promise.allSettled ([acquireBeforeConnection (), acquireAfterConnection ()])
        //         .then (conResults => {
        //             const connection = (transaction.node === 'before') ? beforeConnection : afterConnection;
        //             console.log ("tracsaciont tp be repliacated to " + transaction.node + " node:");
        //             console.log (transaction);
        //             nodeQueue.shift ();
        //             performQuery (connection, transaction.node, transaction.lockQuery, transaction.lockValues, transaction.query, transaction.values)
        //             .then ((results) => {
        //                 console.log ("succeeded in replicating to " + transaction.node + " node.");
        //             })
        //             .catch (() => {
        //                 nodeQueue.unshift (transaction);
        //                 res.send ("failed to replicate to " + transaction.node + " node.");
        //                 return;
        //             });
        //         })
        //         .catch (() => {
        //             console.log ()
        //             res.send ("failed to replicate to " + transaction.node + " node.");
        //             return;
        //         });   
        //     }

        //     while (mainQueue.length !== 0 && typeof mainConnection !== 'undefined') {
        //         const transaction = mainQueue[0];
        //         acquireMainConnection.then (() => {
        //             mainQueue.shift ();
        //             performQuery (mainConnection, 'main', transaction.lockQuery, transaction.lockValues, transaction.query, transaction.values)
        //             .then ((results) => {
        //                 console.log ("succeeded in replicating to main");
        //             })
        //             .catch (() => {
        //                 mainQueue.unshift (transaction);
        //                 res.send ("failed to replicate to main");
        //                 return;
        //             });
        //         })
        //         .catch (() => {
        //             console.log ("failed to replicate to main");
        //             res.send ("failed to replicate to main");
        //             return;
        //         });   
        //     }
        // });

        // replication v1
        // while (nodeQueue.length !== 0 && )
        //  awaitAllConnections () .then (results => {
        //     while (nodeQueue.length !== 0 || mainQueue.length !== 0) {
        //         if (nodeQueue.length !== 0) {
        //             const transaction = nodeQueue[0];
        //             Promise.allSettled ([acquireBeforeConnection (), acquireAfterConnection ()])
        //             .then (conResults => {
        //                 const connection = (transaction.node === 'before') ? beforeConnection : afterConnection;
        //                 console.log ("tracsaciont tp be repliacated to " + transaction.node + " node:");
        //                 console.log (transaction);
        //                 nodeQueue.shift ();
        //                 performQuery (connection, transaction.node, transaction.lockQuery, transaction.lockValues, transaction.query, transaction.values)
        //                 .then ((results) => {
        //                     console.log ("succeeded in replicating to " + transaction.node + " node.");
        //                 })
        //                 .catch (() => {
        //                     nodeQueue.unshift (transaction);
        //                     res.send ("failed to replicate to " + transaction.node + " node.");
        //                     return;
        //                 });
        //             })
        //             .catch (() => {
        //                 console.log ()
        //                 res.send ("failed to replicate to " + transaction.node + " node.");
        //                 return;
        //             });   
        //         }

        //         if (mainQueue.length !== 0 && typeof mainConnection) {
        //             const transaction = mainQueue[0];
        //             acquireMainConnection.then (() => {
        //                 mainQueue.shift ();
        //                 performQuery (mainConnection, 'main', transaction.lockQuery, transaction.lockValues, transaction.query, transaction.values)
        //                 .then ((results) => {
        //                     console.log ("succeeded in replicating to main");
        //                 })
        //                 .catch (() => {
        //                     mainQueue.unshift (transaction);
        //                     res.send ("failed to replicate to main");
        //                     return;
        //                 });
        //             })
        //             .catch (() => {
        //                 console.log ("failed to replicate to main");
        //                 res.send ("failed to replicate to main");
        //                 return;
        //             });   
        //         }
        //     }

        //     console.log ("all nodes are up to date");
        //     res.send ("all nodes are up to date");
        //     return;
        // });
    }
};

module.exports = dbController;