const mainController = {
    getMainPage: (req, res) => {
        res.render ("index");
    },

    getEdit: (req, res) => {
        console.log (req.query);
        res.render ("editMovie", {id: req.query.id, name: req.query.name, year: req.query.year, rank: req.query.rank});
    },

    getAdd: (req, res) => {
        res.render ("newMovie");
    },

    renderSearch: (req, res) => {
        res.render ('partials/searchResults', {results: req.body.results}, (err, html) => {
            res.send (html);
        });
    }
};

module.exports = mainController;