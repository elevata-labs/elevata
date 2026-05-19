"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025-2026 Ilona Tag

This file is part of elevata.

elevata is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of
the License, or (at your option) any later version.

elevata is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with elevata. If not, see <https://www.gnu.org/licenses/>.

Contact: <https://github.com/elevata-labs/elevata>.
"""

from __future__ import annotations

import json

from metadata.architecture.approval import (
  ArchitectureApprovalArtifact,
  ArchitectureApprovalCheckResult,
)
from metadata.architecture.promotion import ArchitecturePromotionReport
from metadata.architecture.report import ArchitectureChangeReport


def render_architecture_report_json(report: ArchitectureChangeReport) -> str:
  """
  Render an architecture change report as deterministic JSON.
  """
  return json.dumps(
    report.to_dict(),
    ensure_ascii=False,
    sort_keys=True,
    indent=2,
  ) + "\n"


def render_architecture_report_text(report: ArchitectureChangeReport) -> str:
  """
  Render an architecture change report as deterministic text.
  """
  lines: list[str] = []

  lines.append("Architecture Change Report")
  lines.append("=" * 26)
  lines.append("")

  lines.append("Scope")
  lines.append("-----")
  lines.append(f"mode: {report.scope.mode}")
  if report.scope.schema_short:
    lines.append(f"schema: {report.scope.schema_short}")
  if report.scope.target_name:
    lines.append(f"target: {report.scope.target_name}")
  lines.append(f"datasets: {len(report.scope.dataset_keys)}")
  for dataset_key in report.scope.dataset_keys:
    lines.append(f"- {dataset_key}")
  lines.append("")

  lines.append("State")
  lines.append("-----")
  lines.append(f"previous_fingerprint: {report.previous_fingerprint or '<none>'}")
  lines.append(f"current_fingerprint: {report.current_fingerprint}")
  lines.append(f"has_changes: {str(report.has_changes).lower()}")
  lines.append(f"report_fingerprint: {report.report_fingerprint}")
  lines.append("")

  lines.append("Summary")
  lines.append("-------")
  lines.append(f"dataset_changes: {report.summary.dataset_change_count}")
  lines.append(f"column_changes: {report.summary.column_change_count}")
  lines.append(f"migration_actions: {report.summary.migration_action_count}")
  lines.append(f"policy_decisions: {report.summary.policy_decision_count}")
  lines.append(f"blocking_policy_decisions: {report.summary.blocking_policy_decision_count}")
  lines.append(f"is_blocked: {str(report.is_blocked).lower()}")
  lines.append("")

  lines.append("Dataset Changes")
  lines.append("---------------")
  if report.dataset_changes:
    for change in report.dataset_changes:
      lines.append(
        _format_dataset_change_line(change)
      )
  else:
    lines.append("- none")
  lines.append("")

  lines.append("Column Changes")
  lines.append("--------------")
  if report.column_changes:
    for change in report.column_changes:
      lines.append(
        _format_column_change_line(change)
      )
  else:
    lines.append("- none")
  lines.append("")

  lines.append("Migration Actions")
  lines.append("-----------------")
  if report.migration_actions:
    for action in report.migration_actions:
      lines.append(f"- {action.to_summary_line()}")
  else:
    lines.append("- none")
  lines.append("")

  lines.append("Policy Decisions")
  lines.append("----------------")
  if report.policy_decisions:
    for decision in report.policy_decisions:
      lines.append(
        _format_policy_decision_line(decision)
      )
  else:
    lines.append("- none")
  lines.append("")

  return "\n".join(lines)


def _format_dataset_change_line(change) -> str:
  """
  Format one dataset change line.
  """
  if change.previous_dataset_name:
    return (
      f"- {change.change_type}: {change.previous_dataset_name} "
      f"-> {change.dataset_key}"
    )
  return f"- {change.change_type}: {change.dataset_key}"


def _format_column_change_line(change) -> str:
  """
  Format one column change line.
  """
  if change.previous_column_name:
    return (
      f"- {change.change_type}: {change.dataset_key}."
      f"{change.previous_column_name} -> {change.column_name}"
    )
  return f"- {change.change_type}: {change.dataset_key}.{change.column_name}"


def _format_policy_decision_line(decision) -> str:
  """
  Format one policy decision line.
  """
  parts = [
    f"- {decision.status}",
    decision.code,
    decision.action_type,
    decision.dataset_key,
  ]
  if decision.column_name:
    parts.append(decision.column_name)
  if decision.message:
    parts.append(f"({decision.message})")
  return " ".join(parts)


def render_architecture_approval_json(artifact: ArchitectureApprovalArtifact) -> str:
  """
  Render an architecture approval artifact as deterministic JSON.
  """
  return json.dumps(
    artifact.to_dict(),
    ensure_ascii=False,
    sort_keys=True,
    indent=2,
  ) + "\n"


def render_architecture_approval_text(artifact: ArchitectureApprovalArtifact) -> str:
  """
  Render an architecture approval artifact as deterministic text.
  """
  data = artifact.to_dict()
  report = artifact.report
  review = artifact.review
  state = report.get("state", {})
  scope = report.get("scope", {})
  summary = report.get("summary", {})

  lines: list[str] = []

  lines.append("Architecture Approval Artifact")
  lines.append("=" * 30)
  lines.append("")

  lines.append("Artifact")
  lines.append("--------")
  lines.append(f"approval_id: {data['approval_id']}")
  lines.append(f"artifact_fingerprint: {data['artifact_fingerprint']}")
  lines.append("")

  lines.append("Review")
  lines.append("------")
  lines.append(f"decision: {review.decision}")
  lines.append(f"decided_by: {review.decided_by}")
  lines.append(f"decided_at: {review.decided_at}")
  if review.note:
    lines.append(f"note: {review.note}")
  lines.append("")

  lines.append("Report")
  lines.append("------")
  lines.append(f"type: {report.get('type')}")
  lines.append(f"report_fingerprint: {report.get('report_fingerprint')}")
  lines.append(f"is_blocked: {str(report.get('is_blocked')).lower()}")
  lines.append(f"previous_fingerprint: {state.get('previous_fingerprint') or '<none>'}")
  lines.append(f"current_fingerprint: {state.get('current_fingerprint')}")
  lines.append("")

  lines.append("Scope")
  lines.append("-----")
  lines.append(f"mode: {scope.get('mode')}")
  if scope.get("schema_short"):
    lines.append(f"schema: {scope.get('schema_short')}")
  if scope.get("target_name"):
    lines.append(f"target: {scope.get('target_name')}")
  dataset_keys = list(scope.get("dataset_keys") or [])
  lines.append(f"datasets: {len(dataset_keys)}")
  for dataset_key in dataset_keys:
    lines.append(f"- {dataset_key}")
  lines.append("")

  lines.append("Summary")
  lines.append("-------")
  lines.append(f"dataset_changes: {summary.get('dataset_change_count')}")
  lines.append(f"column_changes: {summary.get('column_change_count')}")
  lines.append(f"migration_actions: {summary.get('migration_action_count')}")
  lines.append(f"policy_decisions: {summary.get('policy_decision_count')}")
  lines.append(
    "blocking_policy_decisions: "
    f"{summary.get('blocking_policy_decision_count')}"
  )
  lines.append("")

  return "\n".join(lines)


def render_architecture_approval_check_json(
  result: ArchitectureApprovalCheckResult,
) -> str:
  """
  Render an architecture approval check result as deterministic JSON.
  """
  return json.dumps(
    result.to_dict(),
    ensure_ascii=False,
    sort_keys=True,
    indent=2,
  ) + "\n"


def render_architecture_approval_check_text(
  result: ArchitectureApprovalCheckResult,
) -> str:
  """
  Render an architecture approval check result as deterministic text.
  """
  lines: list[str] = []

  lines.append("Architecture Approval Check")
  lines.append("=" * 27)
  lines.append("")
  lines.append(f"status: {result.status}")
  lines.append(f"is_valid: {str(result.is_valid).lower()}")
  lines.append(f"message: {result.message}")
  lines.append(f"report_fingerprint: {result.report_fingerprint or '<none>'}")
  lines.append(f"approval_id: {result.approval_id or '<none>'}")
  lines.append(f"artifact_fingerprint: {result.artifact_fingerprint or '<none>'}")
  lines.append("")

  return "\n".join(lines)


def render_architecture_promotion_report_json(report: ArchitecturePromotionReport) -> str:
  """
  Render an architecture promotion report as deterministic JSON.
  """
  return json.dumps(
    report.to_dict(),
    ensure_ascii=False,
    sort_keys=True,
    indent=2,
  ) + "\n"


def render_architecture_promotion_report_text(report: ArchitecturePromotionReport) -> str:
  """
  Render an architecture promotion report as deterministic text.
  """
  lines: list[str] = []

  lines.append("Architecture Promotion Report")
  lines.append("=" * 29)
  lines.append("")

  lines.append("Direction")
  lines.append("---------")
  lines.append(f"source: {report.source_label}")
  lines.append(f"target: {report.target_label}")
  lines.append("")

  lines.append("State")
  lines.append("-----")
  lines.append(f"source_fingerprint: {report.source_fingerprint}")
  lines.append(f"target_fingerprint: {report.target_fingerprint}")
  lines.append(f"has_changes: {str(report.has_changes).lower()}")
  lines.append(f"is_blocked: {str(report.is_blocked).lower()}")
  lines.append(f"promotion_fingerprint: {report.promotion_fingerprint}")
  lines.append("")

  lines.append("Change Report")
  lines.append("-------------")
  rendered_change_report = render_architecture_report_text(report.change_report)
  lines.extend(
    line
    for line in rendered_change_report.rstrip().splitlines()
  )
  lines.append("")

  return "\n".join(lines)