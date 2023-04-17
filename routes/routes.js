const express = require ('express');

const mainController = require ('../controllers/mainController.js');
const dbController = require ("../controllers/dbController.js");

const app = express ();

app.get ('/', mainController.getMainPage);

app.get ('/editMovie', mainController.getEdit);
app.get ('/addMovie', mainController.getAdd);
app.post ('/renderSearch', mainController.renderSearch);

app.get ('/test', dbController.select);
app.post ('/showVals', mainController.showVals);

app.post ('/replicate', dbController.replicate);
app.post ('/select', dbController.select);
app.post ('/insert', dbController.insert);
app.post ('/update', dbController.update);
app.post ('/delete', dbController.delete);

module.exports = app;