"""
elevata - Metadata-driven Data Platform Framework
Copyright © 2025 Ilona Tag

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

from django.db import migrations

def create_default_target_schemas(apps, schema_editor):
  TargetSchema = apps.get_model("metadata", "TargetSchema")

  defaults = [
    {
      "short_name": "raw",
      "display_name": "Raw Landing",
      "description": (
        "Initial landing zone. 1:1 ingestion of source data for auditability and reloadability. "
        "No business logic, minimal/no transformation."
      ),
      "database_name": "default",
      "schema_name": "raw",
      "physical_prefix": "raw",
      "generate_layer": True,
      "consolidate_groups": False,
      "is_user_visible": False,
      "default_materialization_type": "table",
      "default_historize": False,
      "incremental_strategy_default": "full",
      "sensitivity_default": "public",
      "access_intent_default": "forensic_ingest",
      "surrogate_keys_enabled": False,
      "surrogate_key_algorithm": "sha256",
      "surrogate_key_null_token": "null_replaced",
      "surrogate_key_pair_separator": "~",
      "surrogate_key_component_separator": "|",
      "pepper_strategy": "runtime_pepper",
      "is_system_managed": True,
    },
    {
      "short_name": "stage",
      "display_name": "Staging",
      "description": (
        "Lightly standardized, source-aligned structures. Used to unify structurally similar "
        "source datasets (e.g. multiple SAP clients or regions) into one logical view."
      ),
      "database_name": "default",
      "schema_name": "stage",
      "physical_prefix": "stg",
      "generate_layer": True,
      "consolidate_groups": True,
      "is_user_visible": True,
      "default_materialization_type": "view",
      "default_historize": False,
      "incremental_strategy_default": "full",
      "sensitivity_default": "public",
      "access_intent_default": "staging_alignment",
      "surrogate_keys_enabled": False,
      "surrogate_key_algorithm": "sha256",
      "surrogate_key_null_token": "null_replaced",
      "surrogate_key_pair_separator": "~",
      "surrogate_key_component_separator": "|",
      "pepper_strategy": "runtime_pepper",
      "is_system_managed": True,
    },
    {
      "short_name": "rawcore",
      "display_name": "Raw Core",
      "description": (
        "Technically cleaned core layer. Deduplicated, typed, and enriched with deterministic "
        "surrogate keys. Still source-driven, but structurally reliable."
      ),
      "database_name": "default",
      "schema_name": "rawcore",
      "physical_prefix": "rc",
      "generate_layer": True,
      "consolidate_groups": True,
      "is_user_visible": True,
      "default_materialization_type": "table",
      "default_historize": True,
      "incremental_strategy_default": "merge",
      "sensitivity_default": "public",
      "access_intent_default": "technical_core",
      "surrogate_keys_enabled": True,
      "surrogate_key_algorithm": "sha256",
      "surrogate_key_null_token": "null_replaced",
      "surrogate_key_pair_separator": "~",
      "surrogate_key_component_separator": "|",
      "pepper_strategy": "runtime_pepper",
      "is_system_managed": True,
    },
    {
      "short_name": "bizcore",
      "display_name": "Business Core",
      "description": (
        "Business core layer. Applies business rules, harmonizes semantics across systems, "
        "and captures slowly changing business attributes."
      ),
      "database_name": "default",
      "schema_name": "bizcore",
      "physical_prefix": "bc",
      "generate_layer": False,
      "consolidate_groups": False,
      "is_user_visible": True,
      "default_materialization_type": "table",
      "default_historize": True,
      "incremental_strategy_default": "full",
      "sensitivity_default": "confidential",
      "access_intent_default": "business_core",
      "surrogate_keys_enabled": True,
      "surrogate_key_algorithm": "sha256",
      "surrogate_key_null_token": "null_replaced",
      "surrogate_key_pair_separator": "~",
      "surrogate_key_component_separator": "|",
      "pepper_strategy": "runtime_pepper",
      "is_system_managed": True,
    },
    {
      "short_name": "serving",
      "display_name": "Serving Layer",
      "description": (
        "Curated, consumption-ready layer for analytics, dashboards, reporting and data products. "
        "Optimized for business consumption."
      ),
      "database_name": "default",
      "schema_name": "serving",
      "physical_prefix": "",
      "generate_layer": False,
      "consolidate_groups": False,
      "is_user_visible": True,
      "default_materialization_type": "view",
      "default_historize": False,
      "incremental_strategy_default": "full",
      "sensitivity_default": "confidential",
      "access_intent_default": "analytics_serving",
      "surrogate_keys_enabled": False,
      "surrogate_key_algorithm": "sha256",
      "surrogate_key_null_token": "null_replaced",
      "surrogate_key_pair_separator": "~",
      "surrogate_key_component_separator": "|",
      "pepper_strategy": "runtime_pepper",
      "is_system_managed": True,
    },
  ]

  for data in defaults:
    """
    get_or_create means:
    - if the layer (eg. rawcore) already exists, it WON'T get overrriden
    - if not, it will be created with the given defaults
    """
    TargetSchema.objects.get_or_create(
      short_name=data["short_name"],
      defaults=data,
    )

def reverse_noop(apps, schema_editor):
  """ We don't delete the schemas automatically because the user has probably modified them. """
  pass

class Migration(migrations.Migration):

  dependencies = [
    ("metadata", "0001_initial"),
  ]

  operations = [
    migrations.RunPython(create_default_target_schemas, reverse_noop),
  ]
