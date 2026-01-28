require("dotenv").config();

const express = require("express");
const axios = require("axios");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const ACQUIRE_URL = process.env.ACQUIRE_URL || "http://localhost:3003";
const PREDICT_URL = process.env.PREDICT_URL || "http://localhost:3002";

// HEALTH
app.get("/health", (req, res) => {
  res.json({ status: "ok", service: "orchestrator" });
});

// READY (orchestrator ready si acquire + predict están ready)
app.get("/ready", async (req, res) => {
  try {
    const [acq, pred] = await Promise.allSettled([
      axios.get(`${ACQUIRE_URL}/ready`, { timeout: 5000 }),
      axios.get(`${PREDICT_URL}/ready`, { timeout: 5000 }),
    ]);

    const acquireReady =
      acq.status === "fulfilled" ? !!acq.value.data?.ready : false;
    const predictReady =
      pred.status === "fulfilled" ? !!pred.value.data?.ready : false;

    const ready = acquireReady && predictReady;

    return res.status(200).json({
      ready,
      services: {
        acquire: {
          url: `${ACQUIRE_URL}/ready`,
          ok: acq.status === "fulfilled",
          ready: acquireReady,
          details:
            acq.status === "fulfilled"
              ? acq.value.data
              : (acq.reason?.response?.data || acq.reason?.message || "error"),
        },
        predict: {
          url: `${PREDICT_URL}/ready`,
          ok: pred.status === "fulfilled",
          ready: predictReady,
          details:
            pred.status === "fulfilled"
              ? pred.value.data
              : (pred.reason?.response?.data || pred.reason?.message || "error"),
        },
      },
    });
  } catch (err) {
    return res.status(500).json({
      ready: false,
      error: "ORCH_READY_ERROR",
      details: err.message,
    });
  }
});

// RUN (pipeline completo)
app.post("/run", async (req, res) => {
  try {
    // 1) Llamar a ACQUIRE
    const acquireBody = {};
    if (req.body && req.body.targetDate) acquireBody.targetDate = req.body.targetDate;

    const acquireResp = await axios.post(`${ACQUIRE_URL}/data`, acquireBody, {
      timeout: 20000,
    });

    const { dataId, features } = acquireResp.data || {};

    if (!dataId || !Array.isArray(features) || features.length !== 7) {
      return res.status(502).json({
        error: "BAD_ACQUIRE_RESPONSE",
        message: "Acquire no devolvió dataId/features válidos",
        acquireResp: acquireResp.data,
      });
    }

    // 2) Llamar a PREDICT
    const predictBody = {
      features,
      meta: {
        featureCount: 7,
        dataId,
        source: "orchestrator",
      },
    };

    const predictResp = await axios.post(`${PREDICT_URL}/predict`, predictBody, {
      timeout: 20000,
    });

    // 3) Respuesta final
    return res.status(200).json({
      dataId,
      features,
      ...predictResp.data,
    });
  } catch (err) {
    const status = err.response?.status || 500;
    const details = err.response?.data || err.message;

    return res.status(status).json({
      error: "PIPELINE_ERROR",
      message: "Error ejecutando el pipeline",
      details,
    });
  }
});

app.listen(PORT, () => {
  console.log(`Orchestrator running on port ${PORT}`);
  console.log(`Acquire URL: ${ACQUIRE_URL}`);
  console.log(`Predict URL: ${PREDICT_URL}`);
});
