const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const mysql = require('mysql2');
const routes = require ('./routes/routes.js');
const hbs = require ('hbs');

const app = express();

var corOption = {
    origin: "http://localhost:8080"
}

app.use (cors(corOption));

app.use (bodyParser.json());
app.use (bodyParser.urlencoded({extended: true}));

app.set ('view engine', 'hbs');
hbs.registerPartials (__dirname) + '/views/partials';

app.use (express.static ('public'));
app.use ('/', routes);


const PORT = process.env.PORT || 8080;

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
