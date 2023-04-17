const express = require ('express');

const mainController = require ('../controllers/mainController.js');
const dbController = require ("../controllers/dbController.js");

const app = express ();

app.get ('/', mainController.getMainPage);

app.get ('/editMovie', mainController.getEdit);
app.get ('/addMovie', mainController.getAdd);

app.get ('/test', dbController.select)
app.post ('/select', dbController.select);
app.post ('/insert', dbController.insert);
app.post ('/update', dbController.update);
app.post ('/delete', dbController.delete);

module.exports = app;