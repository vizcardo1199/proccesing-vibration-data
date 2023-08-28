var CronJob = require("cron").CronJob;
const mysql = require("mysql2/promise");
const moment = require("moment");
const logger = require('pino')()

require('dotenv').config();
// Configuración de las credenciales de la base de datos
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

var job = new CronJob(
  "10 * * * * *",
  async function() {
    const candidates = await getCantidates();
    if (candidates?.length > 0) {
      logger.info(`All candidates ${candidates.length}`);
      candidates.forEach(async candidate => {
        logger.info(`Starting with survey ${candidate["row_survey"]}`);
        const pointsBySurvey = await getAllPoints(candidate["row_survey"]);
        const points = await getPoints(candidate["row_survey"]);
        points?.forEach(async point => {
          logger.info(`Checking point: ${point["row_point"]}`);
          const dataForCalculate = getData(pointsBySurvey, point["row_point"]);
          const integral = await calculateIntegral(
            dataForCalculate,
            point["row_point"],
            candidate["row_survey"]
          );
          logger.info(`Integral calculate at point: ${point["row_point"]} DONE`);
          if (integral != undefined) {
            logger.info(`Trying insert: ${point["row_point"]} DONE`);
            await insertVelocity(integral["velocity"],candidate["row_survey"]);
            await insertDisplacement(integral["displacement"],candidate["row_survey"]);
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
// job.start() - See note below when to use this

async function getCantidates() {
  const connection = await mysql.createConnection(dbConfig);
  try {
    // Conexión a la base de datos
    // Consulta para obtener los datos del vector de aceleración
    const query =
      "SELECT DISTINCT s.row_survey FROM surveys s inner join waveforms_acceleration d ON d.row_survey  = s.row_survey WHERE s.state AND s.sv_waveform_processed  AND NOT s.sv_wf_integrated_displacement AND NOT s.sv_wf_integrated_velocity and s.create_date >='2023-08-01' AND s.sv_end_date < date_add(now(), interval fn_get_gmt(s.sv_gmt) hour) limit 10";
    const [rows] = await connection.execute(query);

    // Cerrar la conexión a la base de datos
    connection.end();
    // Verificar que se obtengan datos del vector de aceleración
    if (rows.length === 0) {
      return;
    }
    return rows;
  } catch (error) {
    logger.error
("Error al conectar a la base de datos:", error);
  }
  finally{
    if(connection){
     logger.warn('Killing connection')
     connection.end(); 
    }
  }
 
}

async function getAllPoints(surveyId) {
  const connection = await mysql.createConnection(dbConfig);
  try {
    // Conexión a la base de datos

    // Consulta para obtener los datos del vector de aceleración

    const query =
      "SELECT wfa_measure_y, row_point, wfa_measure_x FROM waveforms_acceleration WHERE row_survey =" +
      surveyId;

    const [rows] = await connection.execute(query);

    // Cerrar la conexión a la base de datos
    connection.end();
    // Verificar que se obtengan datos del vector de aceleración
    if (rows.length === 0) {
      logger.info("No se encontraron puntos en getAllPoints con ", surveyId);
      return;
    }
    return rows;
  } catch (error) {
    logger.error
("Error al conectar a la base de datos:", error);
  }
  finally{
    if(connection){
     logger.info('Killing connection')
     connection.end(); 
    }
  }
}

async function calculateIntegral(rows, point, surveyId) {
  dateString = Math.floor(new Date().getTime() / 1000);
  // Verificar que se obtengan datos del vector de aceleración
  if (rows.length === 0) {
    logger.info("No se encontraron datos del vector de aceleración.");
    return;
  }

  // Convertir los valores del vector de aceleración a números
  const vectorAceleracion = rows.map(row => parseFloat(row.wfa_measure_y));
  if (vectorAceleracion.some(valor => isNaN(valor) || !isFinite(valor))) {
    logger.info(
      "Hay valores no numéricos o inválidos en el vector de aceleración."
    );
    return;
  }
  // Calcular el valor promedio del vector de aceleración
  const sumaAceleracion = vectorAceleracion.reduce((a, b) => a + b, 0);
  const promedioAceleracion = sumaAceleracion / vectorAceleracion.length;

  // Eliminar el DC offset del vector de aceleración
  const vectorAceleracionSinDC = vectorAceleracion.map(
    valor => valor - promedioAceleracion
  );

  // ...

  // Calcular el vector de velocidad (integración utilizando el método del punto medio)
  // Calcular el vector de velocidad (integración utilizando el método de Runge-Kutta de cuarto orden)
  // Calcular el vector de velocidad (integración utilizando el método de Euler mejorado)
  const vectorVelocidad = [0];
  for (let i = 1; i < vectorAceleracionSinDC.length; i++) {
    const deltaTime = 1; // Intervalo de tiempo (se asume constante en este ejemplo)
    const acceleration = vectorAceleracionSinDC[i - 1];

    const k1 = acceleration;
    const k2 = vectorAceleracionSinDC[i];

    const velocity = vectorVelocidad[i - 1] + deltaTime / 2 * (k1 + k2);
    vectorVelocidad.push(velocity);
  }

  // Calcular el valor promedio del vector de aceleración
  const sumaVelocidad = vectorVelocidad.reduce((a, b) => a + b, 0);
  const promedioVelocidad = sumaVelocidad / vectorVelocidad.length;

  // Eliminar el DC offset del vector de aceleración
  const vectorVelocidadSinDC = vectorVelocidad.map(
    valor => valor - promedioVelocidad
  );

  // Calcular el vector de desplazamiento (integración utilizando el método de Euler mejorado)
  const vectorDesplazamiento = [0];
  for (let i = 1; i < vectorVelocidadSinDC.length; i++) {
    const deltaTime = 1; // Intervalo de tiempo (se asume constante en este ejemplo)
    const velocity = vectorVelocidadSinDC[i - 1];

    const k1 = velocity;
    const k2 = vectorVelocidadSinDC[i];

    const displacement =
      vectorDesplazamiento[i - 1] + deltaTime / 2 * (k1 + k2);
    vectorDesplazamiento.push(displacement);
  }
  resultVelocidad = [];
  resultDespla = [];
  vectorVelocidad.forEach((vector, index) => {
    resultVelocidad.push([
      point,
      surveyId,
      dateString,
      vector,
      rows[index].wfa_measure_x,
      "MONITOR",
      1,
      null,
      null,
      new Date()
    ]);
  });

  vectorDesplazamiento.forEach((vector, index) => {
    resultDespla.push([
      point,
      surveyId,
      dateString,
      vector,
      rows[index].wfa_measure_x,
      "MONITOR",
      1,
      null,
      null,
      new Date()
    ]);
  });

  const result = {
    velocity: resultVelocidad,
    displacement: resultDespla
  };
  return result;
}

async function getPoints(surveyId) {
  const connection = await mysql.createConnection(dbConfig);
  try {

    // Consulta para obtener los datos del vector de aceleración
    const query =
      "SELECT DISTINCT row_point FROM waveforms_acceleration WHERE row_survey =" +
      surveyId;
    const [rows] = await connection.execute(query);
  
    // Cerrar la conexión a la base de datos
    connection.end();
  
    // Verificar que se obtengan datos del vector de aceleración
    if (rows.length === 0) {
      logger.info("No se encontraron datos del vector de aceleración.");
      return;
    }
    return rows;
  } catch (error) {
    logger.error
(error)
  }
  finally{
    if(connection){
      logger.warn('Killing connection')
     connection.end(); 
    }
  }
 
  
}

function getData(list, point) {
  return list.filter(obj => {
    return obj["row_point"] === point;
  });
}
async function insertVelocity(data,surveyId) {
  logger.info
(`Inserting data: ${data.length} in survey ${surveyId}`);
  const connection = await mysql.createConnection(dbConfig);
  try {
    let sql = `insert into waveforms_velocity (row_point, row_survey, wfv_timestamp, wfv_measure_y, wfv_measure_x, create_user, state, update_date, update_user, create_date)  VALUES ?`;
    connection.query(sql, [data], (err, result) => {
      if (err) {
        logger.error
("Error al insertar en la tabla MQTT topics:", err);
      } else {
        logger.info("Lote de mensajes MQTT insertado:", result.affectedRows);
      }
    });
    const date = moment(new Date()).format("YYYY-MM-DD HH:mm:ss");
    const query = `UPDATE surveys SET sv_wf_integrated_velocity = 1, update_user= 'MONITOR', update_date = '${date}' WHERE row_survey=${surveyId};`;

    await connection.execute(query);
  } catch (error) {
    logger.info("Error intentado insertar datos:", error);
  } finally {
    if (connection) {
      logger.warn('Killing connection')
      connection.end();
    }
  }
}

async function insertDisplacement(data,surveyId) {
  logger.info
(`Inserting data: ${data.length} in survey ${surveyId}`);
  const connection = await mysql.createConnection(dbConfig);
  try {
    let sql = `insert into waveforms_displacement (row_point, row_survey, wfd_timestamp, wfd_measure_y, wfd_measure_x, create_user, state, update_date, update_user, create_date)  VALUES ?`;
    connection.query(sql, [data], (err, result) => {
      if (err) {
        logger.error
("Error al insertar en la tabla MQTT topics:", err);
      } else {
        logger.info("Lote de mensajes MQTT insertado:", result.affectedRows);
      }
    });
    const date = moment(new Date()).format("YYYY-MM-DD HH:mm:ss");
    const query = `UPDATE surveys SET sv_wf_integrated_displacement = 1, update_user= 'MONITOR', update_date = '${date}' WHERE row_survey=${surveyId};`;

    await connection.execute(query);
  } catch (error) {
    logger.error("Error intentado insertar datos:", error);
  } finally {
    if (connection) {
      logger.warn('Killing connection')
      connection.end();
    }
  }
}
