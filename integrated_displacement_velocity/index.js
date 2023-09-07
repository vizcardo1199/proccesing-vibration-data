var CronJob = require("cron").CronJob;
const logger = require('pino')()
const dotenv = require("dotenv")
const utils = require("./utility")
dotenv.config({ path: "../.env"})


var job = new CronJob(
  "10 * * * * *",
  async function() {
    const candidates = await utils.getCantidates();
    if (candidates?.length > 0) {
      logger.info(`All candidates ${candidates.length}`);
      candidates.forEach(async candidate => {
        logger.info(`Starting with survey ${candidate["row_survey"]}`);
        const pointsBySurvey = await utils.getAllPoints(candidate["row_survey"]);
        const points = await utils.getPoints(candidate["row_survey"]);
        points?.forEach(async point => {
          logger.info(`Checking point: ${point["row_point"]}`);
          const dataForCalculate = getData(pointsBySurvey, point["row_point"]);
          const integral = await utils.calculateIntegral(
            dataForCalculate,
            point["row_point"],
            candidate["row_survey"]
          );
          logger.info(`Integral calculate at point: ${point["row_point"]} DONE`);
          if (integral != undefined) {
            logger.info(`Trying insert: ${point["row_point"]} DONE`);
            await utils.insertVelocity(integral["velocity"],candidate["row_survey"]);
            await utils.insertDisplacement(integral["displacement"],candidate["row_survey"]);
          }
        });
      });
    } else {
      logger.warn("No candidates")
    }
  },
  null,
  true,
  "America/Los_Angeles"
);


function getData(list, point) {
  return list.filter(obj => {
    return obj["row_point"] === point;
  });
}

