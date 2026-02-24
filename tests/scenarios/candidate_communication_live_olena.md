# Live E2E Test Case: Candidate Communication (Forced Olena)

## ID
`live_comm_forced_olena_v1`

## Goal
Validate that recruiter-agent communication from Nick account is autonomous and continuous:

1. If candidate does not reply, agent sends follow-up ping.
2. If candidate replies/questions, agent answers and continues dialog until CV is received or candidate declines.

## Preconditions

1. Service is deployed and healthy (`GET /health` = `200`).
2. Forced test candidate file includes `olena-bachek-b8523121a`.
3. Job workflow completed at least through outreach and created conversation for Olena.
4. `OPENAI_API_KEY` is configured (for non-template LLM replies).
5. Inbound from LinkedIn is wired:
- Preferred: Unipile webhook calls `/api/webhooks/unipile`.
- Fallback: periodic polling + routing into `/api/conversations/{id}/inbound`.

## Test Data

1. Candidate: `olena-bachek-b8523121a`.
2. Recruiter sender: Nick LinkedIn account.
3. Example candidate questions:
- `"Tell me more"`
- `"What timeline?"`
- `"What salary range?"`

## Scenario A: No Candidate Reply -> Follow-up Ping

### Steps

1. Run workflow for a job where Olena is in outreach list.
2. Confirm initial outbound from Nick was sent to Olena.
3. Do not send any reply from Olena.
4. Wait until `next_followup_at` (or force follow-up endpoint/cron cycle).

### Expected Result

1. System generates follow-up outbound to same conversation (Nick -> Olena).
2. Follow-up asks for CV/resume and keeps dialog open.
3. `pre_resume_sessions.status` remains non-terminal (`awaiting_reply` or `engaged_no_resume`).
4. Operation logs include follow-up event (e.g. `followup_sent` / `agent.pre_resume.reply`).

## Scenario B: Candidate Replies -> Agent Continues Dialog

### Steps

1. In the same LinkedIn thread, send from Olena: `"Tell me more"`.
2. Wait for webhook/polling cycle.
3. Send second candidate message (e.g. `"What timeline?"`).

### Expected Result

1. Each inbound candidate message is persisted as inbound event.
2. Agent replies automatically from Nick account to each message.
3. Replies are contextual (JD + instruction + thread context), not dead-end.
4. For pre-resume stage, each reply includes clear CV CTA until CV is received.
5. Logs show inbound + reply chain:
- `conversation.inbound.received`
- `agent.llm.reply` (or fallback trace)
- `agent.pre_resume.reply` (delivery details)

## Scenario C: Candidate Shares CV -> Terminal Transition

### Steps

1. Candidate sends resume link/message:
- `"Here is my CV https://example.com/olena_cv.pdf"`
2. Wait for processing.

### Expected Result

1. Session status moves to `resume_received`.
2. Candidate match status updates to `resume_received`.
3. Resume link is stored in session state/events.
4. No additional CV reminder follow-ups are scheduled.

## Fail Criteria

Test is **FAILED** if any of these occur:

1. Candidate inbound message appears in LinkedIn but no automated Nick reply is sent.
2. Follow-up is not sent after no-reply window.
3. Agent responds once, then dialog does not continue on next candidate message.
4. Reply does not include CV CTA while session is pre-resume and CV not yet received.
5. No corresponding operational logs/events for inbound and outbound actions.

## Debug Checklist (if failed)

1. Verify webhook delivery to `/api/webhooks/unipile` (or polling worker health).
2. Check `GET /api/logs?limit=...` for routing/LLM/delivery errors.
3. Check conversation state:
- `GET /api/conversations/{conversation_id}/messages`
- `GET /api/pre-resume/sessions/{session_id}`
4. Confirm Unipile outbound/connect APIs are reachable for current account permissions.
