const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const mysql = require('mysql2');

const app = express();

var corOption = {
    origin: "http://localhost:8080"
}

const connection = mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: '1234567890',
    database: 'imdb_ijs'
});


connection.query('SELECT * FROM movies', (err, rows, fields) => {
    if (!err)
        console.log(rows);
    else
        console.log(err);
});

app.use (cors(corOption));

app.use (bodyParser.json());

app.use (bodyParser.urlencoded({extended: true}));

app.get('/', (req, res) => {
    res.json({message: "Welcome to my application"});
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});