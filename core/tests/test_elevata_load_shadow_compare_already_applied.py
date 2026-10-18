"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025-2026 Ilona Tag

This file is part of elevata.
"""

from types import SimpleNamespace

from metadata.architecture.shadow_compare import make_schema_op_token
from metadata.management.commands.elevata_load import _schema_op_tokens_already_applied


def test_shadow_compare_counts_already_applied_rename_as_fulfilled():
  action = SimpleNamespace(
    action_type="RENAME_COLUMN",
    dataset_key="rawcore.rc_aw_product_model_hist",
    previous_column_name="instructions_txt",
    column_name="instructions_desc",
  )
  warnings = [
    "SCHEMA_ACTION_ALREADY_APPLIED: RENAME_COLUMN requested by MigrationPlan "
    "for rawcore.rc_aw_product_model_hist.instructions_txt -> instructions_desc, "
    "but the physical column is already renamed. Skipping duplicate DDL."
  ]

  assert _schema_op_tokens_already_applied(
    warnings=warnings,
    actions=(action,),
  ) == {
    make_schema_op_token(
      "RENAME_COLUMN",
      ds="rawcore.rc_aw_product_model_hist",
      prev="instructions_txt",
      cur="instructions_desc",
    )
  }


def test_shadow_compare_counts_already_applied_add_as_fulfilled():
  action = SimpleNamespace(
    action_type="ADD_COLUMN",
    dataset_key="rawcore.customer",
    column_name="customer_name",
  )
  warnings = [
    "SCHEMA_ACTION_ALREADY_APPLIED: ADD_COLUMN requested by MigrationPlan "
    "for rawcore.customer.customer_name, but the physical column already exists. "
    "Skipping duplicate DDL."
  ]

  assert _schema_op_tokens_already_applied(
    warnings=warnings,
    actions=(action,),
  ) == {
    make_schema_op_token(
      "ADD_COLUMN",
      ds="rawcore.customer",
      col="customer_name",
    )
  }


def test_shadow_compare_does_not_credit_unrelated_already_applied_action():
  action = SimpleNamespace(
    action_type="RENAME_COLUMN",
    dataset_key="rawcore.customer",
    previous_column_name="name_old",
    column_name="name",
  )
  warnings = [
    "SCHEMA_ACTION_ALREADY_APPLIED: RENAME_COLUMN requested by MigrationPlan "
    "for rawcore.other.name_old -> name, but the physical column is already renamed. "
    "Skipping duplicate DDL."
  ]

  assert _schema_op_tokens_already_applied(
    warnings=warnings,
    actions=(action,),
  ) == set()
