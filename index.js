import express from "express";
import hbs from "hbs";
import routes from "./routes/routes.js";

const app = express ();
const port = 1101;
const hostname = "localhost";

app.use (express.json ());
app.use (express.static ('public'));

app.set ('view engine', 'hbs');
hbs.registerPartials ('./views/partials');
app.use ('/', routes);

app.listen (port, hostname, function () {
    console.log ('Server is running at:');
    console.log ('http://' + hostname + ':' + port);
});
