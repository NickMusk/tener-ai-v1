import express from "express";
import { buildCandidateRoutes } from "./http/candidateRoutes";
import { HttpError } from "./http/httpError";
import { CandidateService } from "./services/candidateService";

export const createApp = (candidateService: CandidateService) => {
  const app = express();
  app.use(express.json());

  app.get("/health", (_req, res) => {
    res.json({
      status: "ok",
      service: "tener-ls-v01",
      timestamp: new Date().toISOString()
    });
  });

  app.use("/api/v1/candidates", buildCandidateRoutes(candidateService));

  app.use((error: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
    if (error instanceof HttpError) {
      res.status(error.statusCode).json({
        error: error.name,
        message: error.message
      });
      return;
    }

    const message = error instanceof Error ? error.message : "Unexpected server error";
    res.status(500).json({
      error: "InternalServerError",
      message
    });
  });

  return app;
};
