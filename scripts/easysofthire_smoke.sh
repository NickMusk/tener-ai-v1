#!/usr/bin/env bash
set -euo pipefail

: "${EASYSOFTHIRE_MAIN_BASE:?set EASYSOFTHIRE_MAIN_BASE}"
: "${EASYSOFTHIRE_INTERVIEW_BASE:?set EASYSOFTHIRE_INTERVIEW_BASE}"
: "${EASYSOFTHIRE_MAIN_TOKEN:?set EASYSOFTHIRE_MAIN_TOKEN}"
: "${EASYSOFTHIRE_INTERVIEW_TOKEN:?set EASYSOFTHIRE_INTERVIEW_TOKEN}"

echo "== health =="
curl -fsS "${EASYSOFTHIRE_MAIN_BASE}/health" | sed -n '1,120p'
echo
curl -fsS "${EASYSOFTHIRE_INTERVIEW_BASE}/health" | sed -n '1,120p'
echo

echo "== branding =="
curl -fsS "${EASYSOFTHIRE_MAIN_BASE}/" | rg -n "EasySoftHire|Hiring Operations|dedicated hiring operations" -N || {
  echo "main landing is not branded as EasySoftHire" >&2
  exit 1
}
echo "main landing branding ok"

echo "== auth checks =="
main_dash_status="$(curl -s -o /dev/null -w '%{http_code}' "${EASYSOFTHIRE_MAIN_BASE}/dashboard")"
if [[ "${main_dash_status}" != "401" ]]; then
  echo "expected main dashboard without token to return 401, got ${main_dash_status}" >&2
  exit 1
fi
echo "main dashboard unauthenticated access blocked"

main_dash_auth_status="$(curl -s -o /dev/null -w '%{http_code}' \
  -H "Authorization: Bearer ${EASYSOFTHIRE_MAIN_TOKEN}" \
  "${EASYSOFTHIRE_MAIN_BASE}/dashboard")"
if [[ "${main_dash_auth_status}" != "200" ]]; then
  echo "expected main dashboard with token to return 200, got ${main_dash_auth_status}" >&2
  exit 1
fi
echo "main dashboard authenticated access ok"

interview_dash_status="$(curl -s -o /dev/null -w '%{http_code}' "${EASYSOFTHIRE_INTERVIEW_BASE}/dashboard")"
if [[ "${interview_dash_status}" != "401" ]]; then
  echo "expected interview dashboard without token to return 401, got ${interview_dash_status}" >&2
  exit 1
fi
echo "interview dashboard unauthenticated access blocked"

interview_dash_auth_status="$(curl -s -o /dev/null -w '%{http_code}' \
  -H "Authorization: Bearer ${EASYSOFTHIRE_INTERVIEW_TOKEN}" \
  "${EASYSOFTHIRE_INTERVIEW_BASE}/dashboard")"
if [[ "${interview_dash_auth_status}" != "200" ]]; then
  echo "expected interview dashboard with token to return 200, got ${interview_dash_auth_status}" >&2
  exit 1
fi
echo "interview dashboard authenticated access ok"

echo "== api index =="
curl -fsS "${EASYSOFTHIRE_MAIN_BASE}/api" | sed -n '1,120p'
echo
curl -fsS "${EASYSOFTHIRE_INTERVIEW_BASE}/api" | sed -n '1,120p'
echo

echo "smoke checks passed"
