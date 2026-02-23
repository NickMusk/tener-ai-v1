# Pre-Resume Communication Service Scenarios

Standalone scenarios for `PreResumeCommunicationService` (not integrated with main workflow yet).

## Core scenarios

1. Start session
- Input: candidate context (name, role, scope, language).
- Expected: `status=awaiting_reply`, outbound resume request, `next_followup_at` is set.

2. Candidate asks question without resume
- Input: inbound message with intent (`salary`, `stack`, `timeline`, or generic).
- Expected: `status=engaged_no_resume`, outbound answer + resume CTA.

3. Candidate shares resume
- Input: inbound message with resume link/file hint.
- Expected: `status=resume_received`, resume links stored, follow-ups disabled.

4. Candidate says "send later"
- Input: inbound message like "will send CV tomorrow".
- Expected: `status=resume_promised`, reminder schedule remains active.

5. Candidate refuses
- Input: "not interested" message.
- Expected: `status=not_interested`, follow-ups disabled.

6. No response follow-up cadence
- Input: repeated `build_followup` calls up to limit.
- Expected: reminders 1..N sent, then `status=stalled`.

7. Unreachable delivery
- Input: `mark_unreachable`.
- Expected: `status=unreachable`, no additional follow-ups.
