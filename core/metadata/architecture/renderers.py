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