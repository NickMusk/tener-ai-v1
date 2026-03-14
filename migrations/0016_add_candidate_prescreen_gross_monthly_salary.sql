ALTER TABLE candidate_prescreens
    ADD COLUMN IF NOT EXISTS salary_expectation_gross_monthly DOUBLE PRECISION;

UPDATE candidate_prescreens
SET salary_expectation_gross_monthly = CASE
    WHEN salary_expectation_min IS NOT NULL AND salary_expectation_max IS NOT NULL
        THEN (salary_expectation_min + salary_expectation_max) / 2.0
    WHEN salary_expectation_min IS NOT NULL
        THEN salary_expectation_min
    WHEN salary_expectation_max IS NOT NULL
        THEN salary_expectation_max
    ELSE salary_expectation_gross_monthly
END
WHERE salary_expectation_gross_monthly IS NULL;
