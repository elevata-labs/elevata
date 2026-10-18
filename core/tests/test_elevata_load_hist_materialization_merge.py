from types import SimpleNamespace

from metadata.management.commands.elevata_load import _merge_materialization_steps


def _step(op, sql):
  return SimpleNamespace(op=op, sql=sql)


def test_hist_merge_preserves_safe_physical_add_column_without_migration_action():
  ensure = _step("ENSURE_SCHEMA", "CREATE SCHEMA rawcore")
  add = _step("ADD_COLUMN", "ALTER TABLE rawcore.rc_aw_product_model_hist ADD instructions_desc VARCHAR(8000)")
  physical_only_type_change = _step("ALTER_COLUMN_TYPE", "ALTER TABLE rawcore.rc_aw_product_model_hist ALTER COLUMN x VARCHAR(100)")

  merged = _merge_materialization_steps(
    physical_steps=[ensure, add, physical_only_type_change],
    migration_steps=[],
  )

  assert merged == [ensure, add]


def test_hist_merge_appends_migration_steps_and_deduplicates_exact_operations():
  ensure = _step("ENSURE_SCHEMA", "CREATE SCHEMA rawcore")
  add = _step("ADD_COLUMN", "ALTER TABLE rawcore.rc_aw_product_model_hist ADD instructions_desc VARCHAR(8000)")
  duplicate_add = _step("ADD_COLUMN", add.sql)
  migration_rename = _step("RENAME_COLUMN", "EXEC sp_rename ...")

  merged = _merge_materialization_steps(
    physical_steps=[ensure, add],
    migration_steps=[duplicate_add, migration_rename],
  )

  assert merged == [ensure, add, migration_rename]

def test_merge_preserves_safe_physical_rename_column_without_migration_action():
  rename = _step(
    "RENAME_COLUMN",
    "EXEC sp_rename N'[rawcore].[rc_aw_product_model_hist].[instructions]', "
    "N'instructions_desc', N'COLUMN';",
  )

  merged = _merge_materialization_steps(
    physical_steps=[rename],
    migration_steps=[],
  )

  assert merged == [rename]

