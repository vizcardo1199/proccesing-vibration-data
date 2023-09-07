var CronJob = require("cron").CronJob;
const mysql = require("mysql2/promise");
const moment = require("moment");
const logger = require('pino')()
const dotenv = require("dotenv")
dotenv.config({ path: "../.env"})

const topic_value = "acceleration_waveforms"
const dbConfig = {
    host: process.env.DATABASE_HOST,
    port: process.env.DATABASE_PORT,
    user: process.env.DATABASE_USERNAME,
    password: process.env.DATABASE_PASSWORD,
    database: process.env.DATABASE_NAME,
    connectTimeout: process.env.DATABASE_CONNECTION_TIMEOUT,
    multipleStatements: true,
    connectionLimit: process.env.DATABASE_MAX_POOL_SIZE
  };

  // var job = new CronJob(
  //   "5 * * * * *",
  const t =  async function () {
      console.log("Starting")
      const last_processed_row = await getLastProcessRow();
      const candidates = await getCantidates(last_processed_row[0]["current"]);
      console.log(candidates)
      return
      if (candidates?.length > 0) {
        logger.info(`All candidates for analisys: ${candidates.length}`);
        candidates.forEach(async candidate => {
          const message = candidate["tp_message"]
          const row_point = message.split(",")[0]
          const row_timestamp = message.split(",")[1].replaceAll("-","").replaceAll(":","")
          const measure_timestamp_gmt =  getTime(message.split(",")[1]);
          const lower_timestamp_gmt = getTimeLow(measure_timestamp_gmt)
          const upper_timestamp_gmt = getTimeUpper(lower_timestamp_gmt)
          const row_measure_x = message.split(",")[2]
          const row_measure_y = message.split(",")[3] * 0.0114000000;
          console.log("row_point",row_point)
          const row_mawoi = await getMawoiByPoint(row_point);
          console.log("row_mawoi",row_mawoi)
        });
      } else {
        logger.warn("No candidates")
      }
  };
  t();
  //   null,
  //   true,
  //   "America/Los_Angeles"
  // );  
  function getTimeLow(value){
    return moment(value).subtract(moment(value).seconds(),'seconds').format('YYYY-MM-DD HH:mm:ss')
  }
  function getTimeUpper(value){
    return moment(value).add(25,'minutes').format('YYYY-MM-DD HH:mm:ss')
  }
  function getTime(value){
    const year = value.split("-")[0]
    const month = value.split("-")[1]
    const day = value.split("-")[2].substring(0,2);
    const time = value.split("-")[2].substring(2,value.split("-")[2].length)
    const dateTime = moment(year+"-"+month+"-"+day + ' ' + time, 'YYYY-MM-DD HH:mm:ss');
    return dateTime.format('YYYY-MM-DD HH:mm:ss')

  }

  async function getMawoiByPoint(row_point) {
    const connection = await mysql.createConnection(dbConfig);
    try {
      // Conexión a la base de datos
      // Consulta para obtener los datos del vector de aceleración
      const query = `SELECT DISTINCT p.row_mawoi FROM points p WHERE p.state AND p.row_point = ${row_point};`;
      console.log(query)
      const [rows] = await connection.execute(query);
      // Cerrar la conexión a la base de datos
      connection.end();
      // Verificar que se obtengan datos del vector de aceleración
      if (rows.length === 0) {
        return;
      }
      return rows;
    } catch (error) {
      console.error(error)
      logger.error("Error getMawoiByPoint:", error);
    }
    finally{
      if(connection){
       logger.warn('Killing connection')
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
      console.error(error)
      logger.error("Error getLastProcessRow:", error);
    }
    finally{
      if(connection){
       logger.warn('Killing connection getLastProcessRow')
       connection.end(); 
      }
    }
   
  }

async function getCantidates(last_processed_row) {
    const connection = await mysql.createConnection(dbConfig);
    try {
      if(!last_processed_row || last_processed_row == null){
        return;
      }
      // Conexión a la base de datos
      // Consulta para obtener los datos del vector de aceleración
      const query = `SELECT row_topic, tp_message FROM mqtt_topics WHERE state AND tp_topic = "${topic_value}" AND row_topic > ${last_processed_row} ORDER BY row_topic LIMIT 1000`;
      const [rows] = await connection.execute(query);
      // Cerrar la conexión a la base de datos
      connection.end();
      // Verificar que se obtengan datos del vector de aceleración
      if (rows.length === 0) {
        return;
      }
      return rows;
    } catch (error) {
      console.error(error)
      logger.error("Error al conectar a la base de datos:", error);
    }
    finally{
      if(connection){
       logger.warn('Killing connection getCantidates')
       connection.end(); 
      }
    }
   
  }