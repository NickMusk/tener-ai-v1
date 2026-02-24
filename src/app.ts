import express from "express";
import path from "path";
import { buildCandidateRoutes } from "./http/candidateRoutes";
import { buildJobDescriptionRoutes } from "./http/jobDescriptionRoutes";
import { HttpError } from "./http/httpError";
import { CandidateService } from "./services/candidateService";
import { JobDescriptionService } from "./services/jobDescriptionService";

export const createApp = (candidateService: CandidateService, jobDescriptionService: JobDescriptionService) => {
  const app = express();
  app.use(express.json());
  app.use(express.static(path.join(process.cwd(), "public")));

  app.get("/health", (_req, res) => {
    res.json({
      status: "ok",
      service: "tener-ls-v01",
      timestamp: new Date().toISOString()
    });
  });

  app.use("/api/v1/candidates", buildCandidateRoutes(candidateService));
  app.use("/api/v1", buildJobDescriptionRoutes(jobDescriptionService));

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
