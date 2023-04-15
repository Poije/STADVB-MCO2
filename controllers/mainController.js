const mainController = {
    getMainPage: (req, res) => {
        res.render ("index");
    },

    getEdit: (req, res) => {
        res.render ("edit");
    },

    getAdd: (req, res) => {
        res.render ("add");
    }
};

export default mainController;