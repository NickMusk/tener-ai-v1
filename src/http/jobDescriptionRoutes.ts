import { Router } from "express";
import { z } from "zod";
import { JobDescriptionService } from "../services/jobDescriptionService";

const createJobDescriptionSchema = z.object({
  title: z.string().min(2),
  company: z.string().min(2),
  location: z.string().optional(),
  keywords: z.string().optional()
});

export const buildJobDescriptionRoutes = (jobDescriptionService: JobDescriptionService): Router => {
  const router = Router();

  router.get("/linkedin/status", (_req, res) => {
    res.json(jobDescriptionService.getLinkedInStatus());
  });

  router.get("/jds", async (_req, res, next) => {
    try {
      res.json({ items: await jobDescriptionService.listJobDescriptions() });
    } catch (error) {
      next(error);
    }
  });

  router.post("/jds", async (req, res, next) => {
    const parsed = createJobDescriptionSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({
        error: "ValidationError",
        message: parsed.error.flatten()
      });
      return;
    }

    try {
      const item = await jobDescriptionService.createJobDescription(parsed.data);
      res.status(201).json(item);
    } catch (error) {
      next(error);
    }
  });

  router.get("/jds/:jobDescriptionId", async (req, res, next) => {
    try {
      res.json(await jobDescriptionService.getJobDescription(req.params.jobDescriptionId));
    } catch (error) {
      next(error);
    }
  });

  router.post("/jds/:jobDescriptionId/steps/linkedin-search", async (req, res, next) => {
    try {
      res.json(await jobDescriptionService.runLinkedInSearch(req.params.jobDescriptionId));
    } catch (error) {
      next(error);
    }
  });

  router.post("/jds/:jobDescriptionId/steps/import-candidates", async (req, res, next) => {
    try {
      res.json(await jobDescriptionService.runImportCandidates(req.params.jobDescriptionId));
    } catch (error) {
      next(error);
    }
  });

  router.post("/jds/:jobDescriptionId/steps/run-verification", async (req, res, next) => {
    try {
      res.json(await jobDescriptionService.runVerification(req.params.jobDescriptionId));
    } catch (error) {
      next(error);
    }
  });

  router.get("/jds/:jobDescriptionId/candidates", async (req, res, next) => {
    try {
      res.json({ items: await jobDescriptionService.getImportedCandidates(req.params.jobDescriptionId) });
    } catch (error) {
      next(error);
    }
  });

  return router;
};
