import { Router } from "express";
import mainController from "../controllers/mainController.js";
import dbController from "../controllers/dbController.js";

const router = Router ();

router.get ('/', mainController.getMainPage);

router.get ('/editMovie', mainController.getEdit);
router.get ('/addMovie', mainController.getAdd);

router.post ('/select', dbController.select);
router.post ('/insert', dbController.insert);
router.post ('/update', dbController.update);
router.post ('/delete', dbController.delete);

export default router;