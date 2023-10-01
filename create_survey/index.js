var CronJob = require("cron").CronJob;
const mysql = require("mysql2/promise");
const moment = require("moment");
const logger = require("pino")();
const dotenv = require("dotenv");
dotenv.config({ path: "../.env" });

const topic_value = "acceleration_waveforms";
const dbConfig = {
  host: process.env.DATABASE_HOST,
  port: process.env.DATABASE_PORT,
  user: process.env.DATABASE_USERNAME,
  password: process.env.DATABASE_PASSWORD,
  database: process.env.DATABASE_NAME,
  connectTimeout: process.env.DATABASE_CONNECTION_TIMEOUT,
  multipleStatements: true,
  connectionLimit: process.env.DATABASE_MAX_POOL_SIZE,
};
taskRunning=false
var job = new CronJob(
  "* * * * * *",  async function() {
    start()
},
null,
true,
"America/Los_Angeles"
);
async function start(){
  if (taskRunning) {
    return
  }
  taskRunning=true

  console.log("Starting");
  let candidatesMawoi = [];
  const last_processed_row = await getLastProcessRow();
  const candidates = await getCantidates(last_processed_row[0]["current"]);
  logger.info("last process", last_processed_row )
  if (candidates?.length > 0) {
    logger.info(`All candidates for analisys: ${candidates.length}`);
    candidates.forEach(async candidate => {
      const message = candidate["tp_message"];
      const row_point = message.split(",")[0];
      const row_timestamp = message
        .split(",")[1]
        .replaceAll("-", "")
        .replaceAll(":", "");
      const measure_timestamp_gmt = getTime(message.split(",")[1]);
      const lower_timestamp_gmt = getTimeLow(measure_timestamp_gmt);
      const upper_timestamp_gmt = getTimeUpper(lower_timestamp_gmt);
      const row_measure_x = message.split(",")[2];
      const row_measure_y = message.split(",")[3] * 0.0114;
      if (candidate["row_point"] && candidate["row_mawoi"]) {
        candidatesMawoi.push({
          row_topic : candidate["row_topic"],
          row_point,
          row_timestamp,
          measure_timestamp_gmt,
          lower_timestamp_gmt,
          upper_timestamp_gmt,
          row_measure_x,
          row_measure_y,
          row_mawoi: candidate["row_mawoi"],
          row_survey: null
        });
      }
    });
    if (candidatesMawoi) {
      const mawois = removeDuplicates(candidatesMawoi.map(item => item.row_mawoi));
      const surveysCandidates = await Promise.all( mawois.map( async mawoi => {

        let surveysCandidates = {done:[], insert:[]};
        const date = moment(new Date()).format("YYYY-MM-DD HH:mm:ss");
        const result = candidatesMawoi.find(x => x.row_mawoi === mawoi);
        const exists =  await checkIfSurveyExists(mawoi,result["measure_timestamp_gmt"]);
        if(!exists) {

          surveysCandidates.insert.push([
            result.row_mawoi, // row_mawoi:
            result.measure_timestamp_gmt, // measure_timestamp_gmt:
            result.lower_timestamp_gmt, // lower_timestamp_gmt:
            result.upper_timestamp_gmt, //  upper_timestamp_gmt:
            false, //sv_is_reference:
            "+0000", //sv_gmt:
            false, //   sv_band_processed:
            false, //    sv_waveform_processed:
            "CREATE_USER_NODE", //   create_user:
            true, // state:
            date //  create_date:
          ]);
        } else {
          
          surveysCandidates.done.push(exists[0])

        }
        return surveysCandidates;

      }) );

     

      //Updateing mawois with Surveys done on DB 
      if (surveysCandidates[0]?.done.length > 0) {
        surveysCandidates[0]?.done.map(x =>{
          console.log("Survey ya existe : row_survey ", x["row_survey"], "row_mawoi ",x["row_mawoi"])
          const row_survey = x["row_survey"]
          const row_mawoi = x["row_mawoi"]
          candidatesMawoi = candidatesMawoi.map(obj => {
            if (obj.row_mawoi === row_mawoi) {
                return { ...obj, row_survey: row_survey };
            }
            return obj;
        });
        })
       
      }
      if (surveysCandidates[0]?.insert.length > 0) {

        logger.info("Insertando nuevo survey ",surveysCandidates[0]?.insert)
        const result = await insertSurvey(surveysCandidates[0]?.insert);
        const surveyId = JSON.parse(JSON.stringify(result))[0].insertId;
        surveysCandidates[0]?.insert.map(x =>{
          const row_mawoi = x[0];
          console.log("Survey creado : row_survey ", surveyId, "row_mawoi ",row_mawoi)
          candidatesMawoi = candidatesMawoi.map(obj => {
            if (obj.row_mawoi === row_mawoi) {
                return { ...obj, row_survey: surveyId };
            }
            return obj;
        });
        })
      }

      let waveforms_acceleration = [];
      candidatesMawoi.forEach(x=>{
        waveforms_acceleration.push([x.row_point,x.row_survey,x.row_timestamp,x.row_measure_x, x.row_measure_y,'CREATE_USER_NODE',true,null,null,new Date()])
      })
      if(waveforms_acceleration.length > 0)
        await insertWaveform(waveforms_acceleration);
      topics_row = removeDuplicates(candidatesMawoi.map(item => item.row_topic));
    
      max_row_topics = Math.max(...topics_row);
      logger.log("new max topics",max_row_topics)
      await updateParameters(max_row_topics,)

    }

    // candidatesMawoi.forEach(async candidate =>{
    //   console.log(candidate)
    // })
  } else {
    logger.warn("No candidates");
  }
  taskRunning=false


}


// start()

//   null,
//   true,
//   "America/Los_Angeles"
// );
function getTimeLow(value) {
  return moment(value)
    .subtract(moment(value).seconds(), "seconds")
    .format("YYYY-MM-DD HH:mm:ss");
}
function getTimeUpper(value) {
  return moment(value).add(25, "minutes").format("YYYY-MM-DD HH:mm:ss");
}
function removeDuplicates(arr) {
  return arr.filter((item, index) => arr.indexOf(item) === index);
}

function getTime(value) {
  const year = value.split("-")[0];
  const month = value.split("-")[1];
  const day = value.split("-")[2].substring(0, 2);
  const time = value.split("-")[2].substring(2, value.split("-")[2].length);
  const dateTime = moment(
    year + "-" + month + "-" + day + " " + time,
    "YYYY-MM-DD HH:mm:ss"
  );
  return dateTime.format("YYYY-MM-DD HH:mm:ss");
}
async function getCandidatesFilterBySurvey(list) {
  const measure_timestamp_gmt = list[0]["measure_timestamp_gmt"];
  logger.info(
    `Initial analysis withmeasure_timestamp_gmt: ${measure_timestamp_gmt}`
  );
  const mawois = removeDuplicates(list.map(item => item.row_mawoi));
  const connection = await mysql.createConnection(dbConfig);
  try {
    // Conexión a la base de datos
    // Consulta para obtener los datos del vector de aceleración
    const query = `SELECT count(s.row_survey) as exist, s.row_mawoi  FROM surveys s WHERE s.state AND s.row_mawoi in (${mawois.join(
      ","
    )}) AND s.sv_init_date <= "${measure_timestamp_gmt}" AND "${measure_timestamp_gmt}" <= s.sv_end_date GROUP  by  row_mawoi`;
    console.log(query);
    const [rows] = await connection.execute(query);
    // Cerrar la conexión a la base de datos
    connection.end();
    // Verificar que se obtengan datos del vector de aceleración
    if (rows.length === 0) {
      return;
    }
    return rows;
  } catch (error) {
    console.error(error);
    logger.error("Error getMawoiByPoint:", error);
  } finally {
    if (connection) {
      logger.warn("Killing connection");
      connection.end();
    }
  }
}

async function getMawoiByPoint(row_point) {
  const connection = await mysql.createConnection(dbConfig);
  try {
    // Conexión a la base de datos
    // Consulta para obtener los datos del vector de aceleración
    const query = `SELECT DISTINCT p.row_mawoi FROM points p WHERE p.state AND p.row_point = ${row_point};`;
    console.log(query);
    const [rows] = await connection.execute(query);
    // Cerrar la conexión a la base de datos
    connection.end();
    // Verificar que se obtengan datos del vector de aceleración
    if (rows.length === 0) {
      return;
    }
    return rows;
  } catch (error) {
    console.error(error);
    logger.error("Error getMawoiByPoint:", error);
  } finally {
    if (connection) {
      logger.warn("Killing connection");
      connection.end();
    }
  }
}
async function getLastProcessRow() {
  const connection = await mysql.createConnection(dbConfig);
  try {
    // Conexión a la base de datos
    // Consulta para obtener los datos del vector de aceleración
    const query = `SELECT CAST(p.pdm_value AS UNSIGNED) AS current FROM pdm_parameters p WHERE p.pdm_name = CONCAT('TOPIC_', UPPER('${topic_value}'));`;
    const [rows] = await connection.execute(query);
    // Cerrar la conexión a la base de datos
    connection.end();
    // Verificar que se obtengan datos del vector de aceleración
    if (rows.length === 0) {
      return;
    }
    return rows;
  } catch (error) {
    console.error(error);
    logger.error("Error getLastProcessRow:", error);
  } finally {
    if (connection) {
      logger.warn("Killing connection getLastProcessRow");
      connection.end();
    }
  }
}

async function  getCantidates(last_processed_row) {
  const connection = await mysql.createConnection(dbConfig);
  try {
    if (!last_processed_row || last_processed_row == null) {
      return;
    }
    // Conexión a la base de datos
    // Consulta para obtener los datos del vector de aceleración
    // const query = `SELECT c.row_topic, c.tp_message, p.row_point as row_point, p.row_mawoi  FROM mqtt_topics c left join points p on p.row_point  = CAST(SPLIT_STRING(tp_message, ",", 1) AS UNSIGNED) inner join surveys s on s.row_mawoi = p.row_mawoi WHERE c.state AND c.tp_topic =  "${topic_value}" AND row_topic >=  ${last_processed_row} and s.sv_init_date  >= DATE_SUB(NOW(),INTERVAL 15 MINUTE) and DATE_ADD(NOW(),INTERVAL 15 MINUTE) >= s.sv_init_date and s.row_survey  is not null and s.state  = 1 ORDER BY row_topic LIMIT 5000`;
    const query = `SELECT DISTINCT c.row_topic, c.tp_message, p.row_point as row_point, p.row_mawoi  FROM mqtt_topics c left join points p on p.row_point  = CAST(SPLIT_STRING(tp_message, ",", 1) AS UNSIGNED) inner join surveys s on s.row_mawoi = p.row_mawoi WHERE c.state AND c.tp_topic =  "${topic_value}" AND row_topic >  ${last_processed_row}  and s.row_survey  is not null and s.state  = 1 ORDER BY row_topic LIMIT 5000`;

    // const query = `SELECT c.row_topic, c.tp_message , p.row_point , p.row_mawoi , s.row_survey ,sv_init_date,sv_end_date FROM mqtt_topics c left join points p on p.row_point  = CAST(SPLIT_STRING(tp_message, ",", 1) AS UNSIGNED) left join surveys s on p.row_mawoi  = s.row_mawoi  and s.state  = 1 WHERE c.state AND c.tp_topic = "${topic_value}" AND row_topic > ${last_processed_row}   ORDER BY row_topic LIMIT 1000`;
    logger.info(query);
    const [rows] = await connection.execute(query);
    // Cerrar la conexión a la base de datos
    connection.end();
    // Verificar que se obtengan datos del vector de aceleración
    if (rows.length === 0) {
      return;
    }
    return rows;
  } catch (error) {
    console.error(error);
    logger.error("Error al conectar a la base de datos:", error);
  } finally {
    if (connection) {
      logger.warn("Killing connection getCantidates");
      connection.end();
    }
  }
}

const insertSurvey = async function(data) {
  logger.info(`Inserting data: ${data} in survey table`);
  const connection = await mysql.createConnection(dbConfig);
  try {
    let sql = `insert into surveys (row_mawoi, sv_measure_timestamp, sv_init_date, sv_end_date, sv_is_reference, sv_gmt, sv_band_processed, sv_waveform_processed, create_user, state, create_date)  VALUES ?`;
    const value = connection.query(sql, [data], (err, result) => {
      if (err) {
        logger.error
("Error al insertar en la tabla MQTT topics:", err);
      } else {
        logger.info("Lote de mensajes MQTT insertado:", result.affectedRows);
      }
    });
    connection.end();
    return value;
    // const query = `select LAST_INSERT_ID()`;

    // const value (await connection.execute(query);
  } catch (error) {
    logger.error("Error intentado insertar datos:", error);
  } finally {
    if (connection) {
      logger.warn("Killing connection");
      connection.end();
    }
  }
};



const insertWaveform = async function(data) {
  logger.info(`Inserting data in waveforms_acceleration`);
  const connection = await mysql.createConnection(dbConfig);
  try {
    let sql = `INSERT INTO waveforms_acceleration (row_point, row_survey, wfa_timestamp, wfa_measure_x, wfa_measure_y, create_user, state, update_date, update_user, create_date)  VALUES ?`;
    const value = connection.query(sql, [data], (err, result) => {
      if (err) {
        logger.error
("Error al insertar en la tabla waveforms_acceleration topics:", err);
      } else {
        logger.info("Lote de mensajes waveforms_acceleration insertado:", result.affectedRows);
      }
    });
    connection.end();
    return value;
    // const query = `select LAST_INSERT_ID()`;

    // const value (await connection.execute(query);
  } catch (error) {
    logger.error("Error intentado insertar datos:", error);
  } finally {
    if (connection) {
      logger.warn("Killing connection");
      connection.end();
    }
  }
};


const updateParameters = async function (value_row_id) {
  logger.info
(`Update Parameters acceleration_waveforms WITH row: ${value_row_id}`);
  const connection = await mysql.createConnection(dbConfig);
  try {
    const date = moment(new Date()).format("YYYY-MM-DD HH:mm:ss");
    const query = `UPDATE pdm_parameters SET pdm_value = CONCAT(${value_row_id}, ''), update_date = '${date}' WHERE pdm_name=CONCAT('TOPIC_', UPPER('${topic_value}'));`;

    await connection.execute(query);
  } catch (error) {
    logger.info("Error al actualizar datos en updateParameters :", error);
    console.error(error)
  } finally {
    if (connection) {
      logger.warn('Killing connection')
      connection.end();
    }
  }
}


async function checkIfSurveyExists(mawoidID,measure_timestamp_gmt) {
  const connection = await mysql.createConnection(dbConfig);
  try {
    // Conexión a la base de datos
    // Consulta para obtener los datos del vector de aceleración
    const query = `SELECT count(s.row_survey) as here, s.row_survey, s.row_mawoi FROM surveys s WHERE s.state AND s.row_mawoi = ${mawoidID} AND s.sv_init_date <= '${measure_timestamp_gmt}' AND '${measure_timestamp_gmt}' <= s.sv_end_date group by row_survey,row_mawoi;`;
    // const query = `SELECT c.row_topic, c.tp_message , p.row_point , p.row_mawoi , s.row_survey ,sv_init_date,sv_end_date FROM mqtt_topics c left join points p on p.row_point  = CAST(SPLIT_STRING(tp_message, ",", 1) AS UNSIGNED) left join surveys s on p.row_mawoi  = s.row_mawoi  and s.state  = 1 WHERE c.state AND c.tp_topic = "${topic_value}" AND row_topic > ${last_processed_row}   ORDER BY row_topic LIMIT 1000`;
    const [rows] = await connection.execute(query);
    // Cerrar la conexión a la base de datos
    connection.end();
    // Verificar que se obtengan datos del vector de aceleración
    if (rows.length === 0) {
      return;
    }
    return rows;
  } catch (error) {
    console.error(error);
    logger.error("Error al conectar a la base de datos:", error);
  } finally {
    if (connection) {
      logger.warn("Killing connection getCantidates");
      connection.end();
    }
  }
}