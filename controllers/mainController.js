const mainController = {
    getMainPage: (req, res) => {
        res.render ("index");
    },

    getEdit: (req, res) => {
        res.render ("edit");
    },

    getAdd: (req, res) => {
        res.render ("newMovie");
    },

    showVals: (req, res) => {
        console.log ("movie details: ");
        console.log (req.body);
    }
};

module.exports = mainController;