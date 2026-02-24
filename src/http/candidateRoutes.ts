import { Router } from "express";
import { z } from "zod";
import { CandidateService } from "../services/candidateService";

const createCandidateSchema = z.object({
  fullName: z.string().min(2),
  dob: z.string().optional(),
  state: z.string().optional(),
  licenseNumber: z.string().optional(),
  deaNumber: z.string().optional()
});

export const buildCandidateRoutes = (candidateService: CandidateService): Router => {
  const router = Router();

  router.get("/", async (_req, res, next) => {
    try {
      res.json({ items: await candidateService.listCandidates() });
    } catch (error) {
      next(error);
    }
  });

  router.post("/", async (req, res, next) => {
    const parsed = createCandidateSchema.safeParse(req.body);

    if (!parsed.success) {
      res.status(400).json({
        error: "ValidationError",
        message: parsed.error.flatten()
      });
      return;
    }

    try {
      const candidate = await candidateService.createCandidate(parsed.data);
      res.status(201).json(candidate);
    } catch (error) {
      next(error);
    }
  });

  router.get("/:candidateId", async (req, res, next) => {
    try {
      const candidate = await candidateService.getCandidate(req.params.candidateId);
      res.json(candidate);
    } catch (error) {
      next(error);
    }
  });

  router.get("/:candidateId/compliance", async (req, res, next) => {
    try {
      const compliance = await candidateService.getCompliance(req.params.candidateId);
      res.json(compliance);
    } catch (error) {
      next(error);
    }
  });

  router.get("/:candidateId/compliance/full", async (req, res, next) => {
    try {
      const full = await candidateService.getFullCompliance(req.params.candidateId);
      res.json(full);
    } catch (error) {
      next(error);
    }
  });

  router.post("/:candidateId/compliance/run", async (req, res, next) => {
    try {
      const job = await candidateService.enqueueTier1Compliance(req.params.candidateId);
      res.status(202).json(job);
    } catch (error) {
      next(error);
    }
  });

  router.post("/:candidateId/compliance/run-sync", async (req, res, next) => {
    try {
      const compliance = await candidateService.runTier1ComplianceNow(req.params.candidateId);
      res.json(compliance);
    } catch (error) {
      next(error);
    }
  });

  router.get("/compliance-jobs/:jobId", async (req, res, next) => {
    try {
      const job = await candidateService.getVerificationJob(req.params.jobId);
      res.json(job);
    } catch (error) {
      next(error);
    }
  });

  return router;
};
