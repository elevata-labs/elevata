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

import os
import hashlib
import logging
from django.db import transaction
from django.db.models import Max, Q
from django.core.exceptions import ValidationError
from django.conf import settings
from django.db import models
from django.utils import timezone
from crum import get_current_user
from generic import display_key
from metadata.generation import naming
from metadata.rendering.builder import build_surrogate_fk_expression

from metadata.constants import (
  TYPE_CHOICES, INGEST_CHOICES, INCREMENT_INTERVAL_CHOICES, DATATYPE_CHOICES, SYSTEM_COLUMN_ROLE_CHOICES,
  MATERIALIZATION_CHOICES, RELATIONSHIP_TYPE_CHOICES, PII_LEVEL_CHOICES, TARGET_DATASET_INPUT_ROLE_CHOICES,
  ACCESS_INTENT_CHOICES, ROLE_CHOICES, SENSITIVITY_CHOICES, ENVIRONMENT_CHOICES, LINEAGE_ORIGIN_CHOICES,
  TARGET_COMBINATION_MODE_CHOICES, BIZ_ENTITY_ROLE_CHOICES, INCREMENTAL_STRATEGY_CHOICES, JOIN_TYPE_CHOICES, 
  OPERATOR_CHOICES, AGGREGATE_MODE_CHOICES, ORDER_BY_DIR_CHOICES, NULLS_PLACEMENT_CHOICES, WINDOW_FUNCTION_CHOICES, 
  WINDOW_ARG_TYPE_CHOICES)
from metadata.generation.validators import SHORT_NAME_VALIDATOR, TARGET_IDENTIFIER_VALIDATOR


class QueryNodeType(models.TextChoices):
  SELECT = "select", "Select"
  AGGREGATE = "aggregate", "Aggregate"
  UNION = "union", "Union"
  # Future:
  WINDOW = "window", "Window"
  CTE = "cte", "CTE"


class AggregateFunction(models.TextChoices):
  SUM = "SUM", "SUM"
  COUNT = "COUNT", "COUNT"
  MIN = "MIN", "MIN"
  MAX = "MAX", "MAX"
  AVG = "AVG", "AVG"
  # Future: COUNT_DISTINCT, STRING_AGG, etc.


class UnionMode(models.TextChoices):
  UNION = "union", "UNION"
  UNION_ALL = "union_all", "UNION ALL"


class AuditFields(models.Model):
  created_at = models.DateTimeField(auto_now_add=True, db_index=True)
  updated_at = models.DateTimeField(auto_now=True, db_index=True)
  created_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, editable=False,
    on_delete=models.SET_NULL, related_name="+")
  updated_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, editable=False,
    on_delete=models.SET_NULL, related_name="+")

  class Meta:
      abstract = True

  def save(self, *args, **kwargs):
    user = get_current_user()
    if user and not user.is_anonymous:
      # created_by only set once during creation
      if not self.pk and not self.created_by:
        self.created_by = user
      self.updated_by = user
    super().save(*args, **kwargs)

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# PartialLoad
# -------------------------------------------------------------------
class PartialLoad(AuditFields):
  name = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR], unique=True,
    help_text="Short code for this partial load. Will be displayed as the load pipeline name."
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Optional description which purpose this partial load is meant for."
  )

  class Meta:
    db_table = "partial_load"
    ordering = ["name"]
    verbose_name_plural = "Partial Loads"

  def __str__(self):
    return self.name

# -------------------------------------------------------------------
# Team
# -------------------------------------------------------------------
class Team(AuditFields):
  name = models.CharField(max_length=30, unique=True,
    help_text="Name of the team, eg. Sales, Finance, ..."
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Optional description of the team."
  )

  class Meta:
    db_table = "team"
    ordering = ["name"]
    verbose_name_plural = "Teams"

  def __str__(self):
    return self.name

# -------------------------------------------------------------------
# Person
# -------------------------------------------------------------------
class Person(AuditFields):
  email = models.EmailField(unique=True,
    help_text="Email address. Used as the unique identifier of this person."
  )
  name = models.CharField(max_length=200,
    help_text="Full name of the person."
  )
  team = models.ManyToManyField(Team, blank=True, related_name="persons", db_table="team_person",
    help_text="The team(s) the person is assigned to."
  )

  class Meta:
    db_table = "person"
    ordering = ["email"]
    verbose_name_plural = "People"

  def __str__(self):
    return self.email

# -------------------------------------------------------------------
# System
# -------------------------------------------------------------------
class System(AuditFields):
  short_name = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR], unique=True,
    help_text=(
      "Physical / concrete system identifier. eg. 'sap', 'nav', 'crm', 'ga4', "
      "'dwhdev', 'dwh'."
    ),
  )
  name = models.CharField(max_length=50,
    help_text="Identifying name of the system. Does not have technical consequences.",
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Business description / semantic meaning of the system.",
  )
  type = models.CharField(max_length=20, choices=TYPE_CHOICES,
    help_text="System type / backend technology. Used for import and adapter logic.",
  )
  target_short_name = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR],
    help_text=(
      "Stable business prefix for stage/rawcore naming (e.g. 'sap', 'crm', 'fi'). "
      "Primarily relevant for source-driven layers."
    ),
  )
  is_source = models.BooleanField(default=True,
    help_text="If checked, this system acts as a source (has datasets, ingestion, etc.).",
  )
  is_target = models.BooleanField(default=False,
    help_text="If checked, this system acts as a target platform (warehouse/lakehouse/etc.).",
  )
  include_ingest = models.CharField(max_length=20, choices=INGEST_CHOICES, default="none",
    help_text=(
      "How/if this system participates in ingestion pipelines. "
      "Only relevant if is_source=True."
    ),
  )
  generate_raw_tables = models.BooleanField(default=False, 
    help_text=(
      "Default policy: create raw landing tables (TargetDatasets in schema 'raw') "
      "for all SourceDatasets in this system. Only relevant if is_source=True."
    ),
  )
  active = models.BooleanField(default=True,
    help_text="System is still considered a live data source / target.",
  )
  retired_at = models.DateTimeField(blank=True, null=True,
    help_text="Automatically set when active is unchecked.",
  )

  class Meta:
    db_table = "system"
    ordering = ["short_name"]
    verbose_name_plural = "Systems"

  def __str__(self):
    return self.short_name

  def save(self, *args, **kwargs):
    # Automatically set retired_at when a row becomes inactive
    if not self.active and self.retired_at is None:
      self.retired_at = timezone.now()
    elif self.active:
      self.retired_at = None
    super().save(*args, **kwargs)


# -------------------------------------------------------------------
# SourceDataset
# -------------------------------------------------------------------
class SourceDataset(AuditFields):
  source_system = models.ForeignKey(System, on_delete=models.CASCADE, related_name="source_datasets",
    help_text="The system this dataset comes from (must have is_source=True).",
    limit_choices_to={"is_source": True},
  )
  schema_name = models.CharField(max_length=50, blank=True, null=True,
    help_text="The schema name this dataset resides on. Can be left empty if it is a default schema (eg. dbo in SQLServer)."
  )
  source_dataset_name = models.CharField(max_length=100,
    help_text="Original name of the source dataset."
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Optional description of the business content in this dataset."
  )
  integrate = models.BooleanField(default=True,
    help_text=(
      "Controls whether this source dataset is in scope for integration into the target platform "
      "(stage/rawcore/bizcore/etc.). "
      "Uncheck if the dataset is documented here but not yet ready or intentionally excluded "
      "from automated target generation."
    )
  )
  static_filter = models.CharField(max_length=255, blank=True, null=True,
    help_text=(
      "Static WHERE clause applied to all loads of this dataset. "
      "Used for permanent business or technical scoping. "
      "Example: is_deleted_flag = 0 AND country_code = 'DE'."
    ),
  )  
  incremental = models.BooleanField(default=False,
    help_text=(
      "If checked, an incremental load strategy will be applied. "
      "In this case, appropriate increment parameters have to be provided."
    )
  )
  increment_filter = models.CharField(max_length=255, blank=True, null=True,
    help_text=(
      "Template WHERE clause for incremental extraction. "
      "Use the placeholder {{DELTA_CUTOFF}} for the dynamic cutoff timestamp/date. "
      "Example: (last_update_ts >= {{DELTA_CUTOFF}} OR created_at >= {{DELTA_CUTOFF}}) "
      "AND is_deleted_flag = 0."
    )
  )
  manual_model = models.BooleanField(default=False,
    help_text="If checked: dataset is manually maintained, not fully auto-generated."
  )
  distinct_select = models.BooleanField(default=False,
    help_text="If checked: SELECT DISTINCT is enforced during generation."
  )
  owner = models.ManyToManyField("Person", blank=True, through="SourceDatasetOwnership", related_name="source_datasets",
    help_text="Declared business / technical owners with roles."
  )
  generate_raw_table = models.BooleanField(default=None, null=True,
    help_text=(
      "If Unknown: inherit the setting on SourceSystem level. "
      "If Yes: force creation of a raw landing TargetDataset for this SourceDataset. "
      "If No: suppress creation of a raw landing TargetDataset."
    )
  )
  active = models.BooleanField(default=True, 
    help_text=(
      "Indicates whether this dataset is still considered an active source in the originating system. "
      "If unchecked, the dataset is treated as retired (no new loads expected) but it remains "
      "in metadata for lineage, audit, and historical reference."
    )
  )
  retired_at = models.DateTimeField(blank=True, null=True,
    help_text="Auto-set when active is unchecked. Used for lineage and audit."
  )

  class Meta:
    db_table = "source_dataset"
    constraints = [
      models.UniqueConstraint(
        fields=["source_system", "schema_name", "source_dataset_name"],
        name="unique_source_dataset"
      )
    ]
    ordering = ["source_system", "schema_name", "source_dataset_name"]
    verbose_name_plural = "Source Datasets"

  def __str__(self):
    return f"({self.source_system}) {display_key(self.schema_name, self.source_dataset_name)}"

  def save(self, *args, **kwargs):
    # Automatically set retired_at when a row becomes inactive
    if not self.active and self.retired_at is None:
      self.retired_at = timezone.now()
    elif self.active:
      # If reactivated, clear retired_at
      self.retired_at = None
    super().save(*args, **kwargs)

  @property
  def missing_increment_policies(self) -> list[str]:
    """
    Return a list of environments that are considered missing.
    Currently this is only meaningful if *no* active policy exists at all.
    """
    if not getattr(self, "incremental", False):
      return []

    if not (getattr(self, "increment_filter", None) or "").strip():
      return []

    active_envs = list(
      self.increment_policies
        .filter(active=True)
        .values_list("environment", flat=True)
    )

    # If nothing is defined at all, this is a problem → surface all envs as missing
    if not active_envs:
      return [e[0] for e in ENVIRONMENT_CHOICES]

    # Otherwise: no "missing" by default (non-strict semantics)
    return []

  @property
  def has_no_missing_increment_policies(self) -> bool:
    """
    True if incremental is either not relevant
    or at least one active increment policy exists.
    """
    if not getattr(self, "incremental", False):
      return True

    if not (getattr(self, "increment_filter", None) or "").strip():
      return True

    return self.increment_policies.filter(active=True).exists()

  @property
  def has_missing_increment_policies(self) -> bool:
    return not self.has_no_missing_increment_policies


# -------------------------------------------------------------------
# SourceDatasetIncrementPolicy
# -------------------------------------------------------------------
class SourceDatasetIncrementPolicy(AuditFields):
  source_dataset = models.ForeignKey(SourceDataset, on_delete=models.CASCADE, related_name="increment_policies",
    help_text="The dataset this policy applies to."
  )
  environment = models.CharField(max_length=20, choices=ENVIRONMENT_CHOICES,
    help_text="Environment this policy applies to (e.g. dev, test, prod)."
  )
  increment_interval_length = models.PositiveIntegerField(
    help_text="How far to look back from 'now' for incremental loads in this environment."
  )
  increment_interval_unit = models.CharField(max_length=10, choices=INCREMENT_INTERVAL_CHOICES,
    help_text="Unit for the increment interval length."
  )
  active = models.BooleanField(default=True,
    help_text="If multiple rows exist (history), which one is currently active."
  )

  class Meta:
    db_table = "source_dataset_increment_policy"
    constraints = [
      models.UniqueConstraint(
        fields=["source_dataset", "environment", "active"],
        name="unique_active_increment_policy_per_env",
      )
    ]
    ordering = ["source_dataset", "environment"]

  def __str__(self):
    return f"{self.source_dataset} [{self.environment}] {self.increment_interval_length} {self.increment_interval_unit} back"
  
# -------------------------------------------------------------------
# SourceDatasetGroup
# -------------------------------------------------------------------
class SourceDatasetGroup(AuditFields):
  target_short_name = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR],
    help_text=(
      "Short code used to derive the unified target table prefix. "
      "eg. sap for grouping sap1 and sap2."
    )
  )
  unified_source_dataset_name = models.CharField(max_length=128,
    help_text="Unified name of the source object. May be one of the source datasets which participate in this group."
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Optional description why the group was established." 
  )
  owner = models.ManyToManyField(Person, blank=True, related_name="source_dataset_groups",
    help_text="Optional governance owner definition. May be used as default owner for generated TargetDatasets."
  )

  class Meta:
    db_table = "source_dataset_group"
    ordering = ["target_short_name", "unified_source_dataset_name"]
    verbose_name_plural = "Source Dataset Groups"

  def __str__(self):
    return f"{self.target_short_name}_{self.unified_source_dataset_name}"

# -------------------------------------------------------------------
# SourceDatasetGroupMembership
# -------------------------------------------------------------------
class SourceDatasetGroupMembership(AuditFields):
  group = models.ForeignKey(SourceDatasetGroup, on_delete=models.CASCADE, related_name="memberships",
    help_text="The source dataset group. Only necessary to store if more than one source datasets should be grouped together."
  )
  source_dataset = models.ForeignKey(SourceDataset, on_delete=models.CASCADE, related_name="dataset_groups",
    help_text="The dataset to be assigned to the group."
  )
  is_primary_system = models.BooleanField(default=False,
    help_text="If checked, this dataset is considered the 'golden' / leading source for this group."
  )
  source_identity_id = models.CharField(max_length=30, blank=True, null=True,
    help_text=(
      "Optional identifier used to distinguish rows when multiple source "
      "datasets are grouped (e.g. 'aw1', 'aw2'). "
      "If set, it can be used as additional business key component or "
      "as tie-breaker in incremental logic."
    ),
  )
  source_identity_ordinal = models.PositiveIntegerField(blank=True, null=True,
    help_text=(
      "Optional priority score for this source within the group. "
      "Lower values mean higher priority. "
      "If not set, the default order uses is_primary_system and identity id."
    ),
  )

  class Meta:
    db_table = "source_dataset_group_membership"
    constraints = [models.UniqueConstraint(fields=["group", "source_dataset"], name="unique_group_membership")]
    ordering = ["-is_primary_system", "group", "source_dataset"] # - means descending

# -------------------------------------------------------------------
# SourceDatasetOwnership
# -------------------------------------------------------------------
class SourceDatasetOwnership(AuditFields):
  source_dataset = models.ForeignKey("SourceDataset", on_delete=models.CASCADE, related_name="source_dataset_ownerships",
    help_text="The dataset for which an owner is declared."
  )
  person = models.ForeignKey("Person", on_delete=models.PROTECT, related_name="source_dataset_ownerships",
    help_text="The person who is declared as owner of the dataset."
  )
  role = models.CharField(max_length=20, choices=ROLE_CHOICES,
    help_text="The role which the person has on the dataset."
  )
  is_primary_owner = models.BooleanField(default=False,
    help_text="If checked, this is the primary ownership."
  )
  since = models.DateField(blank=True, null=True,
    help_text="The date from which the ownership will start."
  )
  until = models.DateField(blank=True, null=True,
    help_text="The date on which the ownership will end."
  )
  remark = models.CharField(max_length=255, blank=True, null=True,
    help_text="Optional remarks concerning the ownership."
  )

  class Meta:
    constraints = [models.UniqueConstraint(fields=["source_dataset", "person", "role"], name="unique_source_dataset_ownership")]
    ordering = ["-is_primary_owner", "source_dataset", "role", "since"]

  def __str__(self):
    return f"{self.source_dataset} · {self.person} ({self.role})"

# -------------------------------------------------------------------
# SourceColumn
# -------------------------------------------------------------------
class SourceColumn(AuditFields): 
  source_dataset = models.ForeignKey(SourceDataset, on_delete=models.CASCADE, related_name="source_columns",
    help_text="The dataset this column belongs to."
  )
  source_column_name = models.CharField(max_length=100,
    help_text="Original source column name, eg. 'MANDT', 'VBELN'."
  )
  ordinal_position = models.PositiveIntegerField(
    help_text="Column order within the dataset."
  )
  source_datatype_raw = models.CharField(max_length=100, blank=True, null=True,
    help_text=(
      "Raw source datatype as reported by the source introspection (lossless). "
      "Example: 'bit', 'nvarchar(max)', 'decimal(18,2)'."
    )
  )
  datatype = models.CharField(max_length=20, choices=DATATYPE_CHOICES,
    help_text="Logical / normalized datatype."
  )
  max_length = models.PositiveIntegerField(blank=True, null=True,
    help_text="How many characters the field may have."
  )
  decimal_precision = models.PositiveIntegerField(blank=True, null=True,
    help_text="If datatype decimal, the decimal precision of the column."
  )
  decimal_scale = models.PositiveIntegerField(blank=True, null=True,
    help_text="If datatype decimal, the number of decimal places of the column."
  )
  nullable = models.BooleanField(default=True,
    help_text="If checked, this column can be NULL in the source dataset."
  )
  primary_key_column = models.BooleanField(default=False,
    help_text="If checked, this column is part of the natural/business key (not the surrogate key)."
  )
  referenced_source_dataset_name = models.CharField(max_length=100, blank=True, null=True,
    help_text=(
      "Name of the upstream source object this column refers to "
      "(e.g. table 'customer_master', API 'GET /customers'). "
      "Serves for information purposes, does not necessarily have "
      "to be integrated/modeled as a SourceDataset. "
      "Used for lineage suggestions and probable future FK generation."
    )
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Business description / semantic meaning of the column."
  )
  integrate = models.BooleanField(default=False,
    help_text= (
      "Controls whether this source column is in scope for integration into the target model. "
      "Uncheck, if the column is not (yet) chosen for integration."
    )
  )
  pii_level = models.CharField(max_length=30, choices=PII_LEVEL_CHOICES, default="none",
    help_text="PII classification of this column, if applicable."
  )
  remark = models.CharField(max_length=255, blank=True, null=True,
    help_text="Free-form notes."
  )

  class Meta:
    db_table = "source_column"
    constraints = [models.UniqueConstraint(fields=["source_dataset", "source_column_name"], name="unique_source_column"),
                   models.UniqueConstraint(fields=["source_dataset", "ordinal_position"], name="unique_source_column_position")]
    ordering = ["source_dataset", "ordinal_position"]
    verbose_name_plural = "Source Columns"

  def __str__(self):
    return display_key(self.source_dataset, self.source_column_name)
  
# -------------------------------------------------------------------
# TargetSchema
# -------------------------------------------------------------------
class TargetSchema(AuditFields):
  short_name = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR], unique=True,
    help_text=(
      "Logical layer identifier. Examples: 'raw', 'stage', 'rawcore', 'bizcore', 'serving'. "
      "Defines architectural intent and default behavior."
    )
  )
  display_name = models.CharField(max_length=50, 
    help_text="Human-readable name, e.g. 'Business Core', 'Serving Layer'."
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Purpose of this layer and what transformations are allowed here."
  )
  database_name = models.CharField(max_length=100, validators=[TARGET_IDENTIFIER_VALIDATOR],
    help_text="Physical target database / catalog on the destination platform."
  )
  schema_name = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR],
    help_text= (
      "Physical schema / namespace on the destination platform. "
      "Defaults to short_name, but can differ if platform naming conventions require it."
    )
  )
  physical_prefix = models.CharField(max_length=10, validators=[SHORT_NAME_VALIDATOR], blank=True, null=True,
    help_text=(
      "Optional physical naming prefix for datasets in this schema, "
      "eg. 'raw', 'stg', 'rc'. Will be prepended like '<prefix>_<sys>_<obj>'. "
      "If empty, no prefix will be added."
    )
  )
  generate_layer = models.BooleanField(
    default=True,
    help_text=(
      "If checked, datasets for this layer are generated automatically "
      "from source metadata (e.g. raw, stage, rawcore). "
      "Uncheck for curated layers (e.g. bizcore, serving)."
    )
  )
  is_user_visible = models.BooleanField(default=True, 
    help_text=(
      "Whether end users can actively model new datasets in this layer. "
      "If unchecked, this layer is internal/technical and hidden in normal UIs."
    )
  )
  # Default technical behavior for datasets in this schema
  default_materialization_type = models.CharField(max_length=30, choices=MATERIALIZATION_CHOICES, default="table",
    help_text="Default materialization strategy for datasets in this layer."
  )
  default_historize = models.BooleanField(default=True, 
    help_text="Whether datasets in this layer are expected to track history / SCD-style state."
  )
  incremental_strategy_default = models.CharField(max_length=20, choices=INCREMENTAL_STRATEGY_CHOICES, default="full",
    help_text=(
      "Default incremental loading strategy for datasets in this schema "
      "when the source is incremental. For non-incremental sources, "
      "a full refresh is always used."
    ),
  )
  # Governance defaults
  sensitivity_default = models.CharField(max_length=30, choices=SENSITIVITY_CHOICES, default="public",
    help_text="Default sensitivity classification for datasets in this schema (e.g. public, confidential)."
  )
  access_intent_default = models.CharField(max_length=30, choices=ACCESS_INTENT_CHOICES, blank=True, null=True,
    help_text="Default usage intent for governance purposes: analytics, finance_reporting, operations, ml_feature_store, etc."
  )
  # Surrogate key policy for this layer
  surrogate_keys_enabled = models.BooleanField(default=True,
    help_text=(
      "If checked, this layer is expected to generate deterministic surrogate keys "
      "for its primary entities. If unchecked, no surrogate keys will be generated."
    )
  )
  surrogate_key_algorithm = models.CharField(max_length=20, default="sha256",
    help_text="Hash algorithm used for deterministic surrogate keys in this layer."
  )
  surrogate_key_null_token = models.CharField(max_length=50, default="null_replaced",
    help_text="Token to use instead of NULL in natural key components."
  )
  surrogate_key_pair_separator = models.CharField(max_length=5, default="~",
    help_text="Separator between field name and value within one component."
  )
  surrogate_key_component_separator = models.CharField(max_length=5, default="|",
    help_text="Separator between components in the natural key string."
  )
  pepper_strategy = models.CharField(max_length=50, default="runtime_pepper",
    help_text="How pepper is injected at runtime. No pepper value is stored in metadata."
  )
  is_system_managed = models.BooleanField(default=False,
    help_text="If checked, this schema is managed by the system and core attributes are locked."
  )

  class Meta:
    db_table = "target_schema"
    ordering = ["short_name"]
    verbose_name_plural = "Target Schemas"

  def __str__(self):
    return f"{self.short_name} ({self.database_name}.{self.schema_name})"

# -------------------------------------------------------------------
# TargetDataset
# -------------------------------------------------------------------
class TargetDataset(AuditFields):
  target_schema = models.ForeignKey(TargetSchema, on_delete=models.PROTECT, related_name="target_datasets",
    help_text=(
      "Which layer / schema this dataset belongs to. "
      "Defines physical DB/schema, default materialization and governance expectations."
    )
  )
  # Logical / business-facing name of the dataset in the target platform
  # NOTE: Naming rules are enforced in forms.py (schema-dependent) and in health/validators.
  # Model field validators are static and cannot depend on target_schema.
  target_dataset_name = models.CharField(max_length=63,
    help_text=(
      "Final dataset (table/view) name, snake_case inc. layer prefix. eg. 'rc_sap_customer', 'rc_sap_sales_order'."
    )
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Business description / semantic meaning of the dataset."
  )
  handle_deletes = models.BooleanField(default=True, 
    help_text=(
      "Whether deletes in the source should be reflected in this dataset. "
      "Only relevant with incremental or historization tables."
    )
  )
  historize = models.BooleanField(default=True,
    help_text="Track slowly changing state / valid_from / valid_to, etc."
  )
  source_datasets = models.ManyToManyField(SourceDataset, through="TargetDatasetInput", 
    through_fields=("target_dataset", "source_dataset"), related_name="target_datasets", blank=True,
    help_text=(
      "Which source datasets feed this target dataset. "
      "Used for multi-source consolidation, staging, rawcore integration, etc."
    )
  )
  upstream_datasets = models.ManyToManyField("self", through="TargetDatasetInput",
    through_fields=("target_dataset", "upstream_target_dataset"), symmetrical=False,
    related_name="downstream_datasets", blank=True,
    help_text=(
      "Which other target dataset feed this target dataset as upstream "
      "instead of source datasets."
    )
  )
  combination_mode = models.CharField(max_length=20, choices=TARGET_COMBINATION_MODE_CHOICES, default="single",
    help_text=(
      "How multiple upstream datasets are combined in the pipeline. "
      "'single' = one upstream, 'union' = append all upstreams."
    ),
  )
  # --- BizCore semantics (mainly used when target_schema.short_name == 'bizcore') ---
  biz_entity_role = models.CharField( max_length=30, choices=BIZ_ENTITY_ROLE_CHOICES, blank=True, null=True,
    help_text=(
      "Semantic role of this dataset in the BizCore model. "
      "Examples: core_entity, fact, dimension, reference."
    ),
  )
  biz_grain_note = models.CharField(max_length=255, blank=True, null=True,
    help_text=(
      "Human-readable description of the business grain. "
      "Example: 'one row per customer and today', 'one row per contract and day'."
    ),
  )
  incremental_strategy = models.CharField(max_length=20, choices=INCREMENTAL_STRATEGY_CHOICES, default="full",
    help_text=(
      "How this dataset is loaded.\n"
      "- full: always full refresh (truncate + reload)\n"
      "- append: append-only incremental load\n"
      "- merge: upsert by business key and handle deletes\n"
      "- snapshot: periodic snapshots by watermark/date\n"
      "- historize: SCD2 history dataset (system-managed history load)"
    ),
  )
  incremental_source = models.ForeignKey(SourceDataset, on_delete=models.SET_NULL, null=True, blank=True, related_name="incremental_targets",
    help_text=(
      "If set, this target dataset inherits incremental window logic (and delete detection scope) "
      "from the referenced SourceDataset. The referenced dataset's increment_filter "
      "defines which records are considered 'in scope' for delta load & delete detection."
    )
  )
  manual_model = models.BooleanField(default=False,
    help_text="If checked: dataset is manually maintained, not fully auto-generated."
  )
  distinct_select = models.BooleanField(default=False,
    help_text="If checked: SELECT DISTINCT is enforced during generation."
  )
  static_filter = models.CharField(max_length=255, blank=True, null=True,
    help_text=(
      "Optional row-level filter to restrict records. "
      "Static WHERE clause applied to all loads of this dataset. "
      "Used for permanent business or technical scoping. "
      "Example: is_deleted_flag = 0 AND country_code = 'DE'."
    ),
  ) 
  query_root = models.ForeignKey("QueryNode", on_delete=models.SET_NULL, null=True, blank=True, related_name="+",
    help_text="Optional query-tree root. If empty, dataset uses the classic select/joins definition.",
  )
  query_head = models.ForeignKey("QueryNode", on_delete=models.SET_NULL, null=True, blank=True, related_name="+",
    help_text=(
      "Optional query-tree head (leaf): the final node that represents the dataset output. "
      "query_root stays stable (Base select); query_head moves when adding wrapper nodes "
      "(aggregate/window/union). If null while query_root is set, treat query_root as head."
    ),
  )
  partial_load = models.ManyToManyField("PartialLoad", blank=True, related_name="datasets", db_table="target_dataset_partial_load",
    help_text="Optional subset extraction definitions (per environment / window)."
  )
  owner = models.ManyToManyField("Person", blank=True, through="TargetDatasetOwnership", related_name="target_datasets",
    help_text="Declared business / technical owners with roles."
  )
  materialization_type = models.CharField(max_length=30, choices=MATERIALIZATION_CHOICES, blank=True, null=True,
    help_text=(
      "If set, overrides target_schema.default_materialization_type for this dataset. "
      "If null, dataset inherits the schema default."
    )
  )
  sensitivity = models.CharField(max_length=30, choices=SENSITIVITY_CHOICES, default="public",
    help_text="Confidentiality / sensitivity level for this dataset."
  )
  access_intent = models.CharField(max_length=30, choices=ACCESS_INTENT_CHOICES, blank=True, null=True,
    help_text="Intended usage for governance purposes, e.g. analytics, finance_reporting, ml_feature_store."
  )
  active = models.BooleanField(default=True, 
    help_text=(
      "If unchecked, this dataset is deprecated. It remains in metadata for lineage "
      "and documentation but should not be generated, deployed, or used for new models."
    )
  )
  retired_at = models.DateTimeField(blank=True, null=True,
    help_text=(
      "Optional timestamp when this dataset was marked as inactive. "
      "Automatically set when active gets unchecked."
    )
  )
  lineage_key = models.CharField(max_length=255, null=True, blank=True, db_index=True,
    help_text=(
      "Stable technical key used by the generator to identify this dataset "
      "by its source lineage instead of its physical name. "
      "Ensures that renaming target_dataset_name does not create duplicates."
    ),
  )
  former_names = models.JSONField(
    blank=True, default=list, help_text=(
      "List of previous physical dataset names for this TargetDataset. "
      "Used by materialization sync to detect renames and emit RENAME TABLE/VIEW "
      "instead of creating a new object. Technical governance field; do not edit manually."
    ),
  )
  is_system_managed = models.BooleanField(default=False,
    help_text="If checked, this dataset is managed by the system and core attributes are locked."
  )

  class Meta:
    db_table = "target_dataset"
    constraints = [
      models.UniqueConstraint(
        fields=["target_schema", "target_dataset_name"],
        name="unique_target_dataset_per_schema",
      )
    ]
    ordering = ["target_schema", "target_dataset_name"]
    verbose_name_plural = "Target Datasets"

  def __str__(self):
    return self.target_dataset_name
  
  @property
  def effective_materialization_type(self):
    # Application-level helper, not stored in DB.
    return self.materialization_type or self.target_schema.default_materialization_type
  
  @property
  def is_incremental(self):
    """
    Returns True if this dataset uses any incremental loading strategy.
    Full refresh ('full') is treated as non-incremental.
    """
    return self.incremental_strategy in {"append", "merge", "snapshot"}

  @property
  def is_hist(self) -> bool:
    return (
      getattr(self.target_schema, "short_name", None) == "rawcore"
      and self.incremental_strategy == "historize"
    )

  @property
  def natural_key_fields(self):
    """
    Returns sorted list of target_column names that are marked as business key column.
    """
    qs = (
      self.target_columns
      .filter(system_role="business_key")
      .values_list("target_column_name", flat=True)
    )
    return sorted(list(qs))
  
  def _active_input_links_qs(self):
    qs = getattr(self, "input_links", None)
    if qs is None:
      return self.input_links.none()
    qs = qs.select_related("source_dataset", "upstream_target_dataset")
    if hasattr(qs.model, "active"):
      qs = qs.filter(active=True)
    return qs.order_by("role", "id")

  def _active_joins_qs(self):
    qs = getattr(self, "joins", None)
    if qs is None:
      return []
    qs = qs.all()
    if hasattr(qs.model, "active"):
      qs = qs.filter(active=True)
    return qs

  @property
  def missing_join_inputs(self) -> list[str]:
    """
    Best-effort: if there are multiple active inputs but no active join defined,
    list the non-base inputs that are effectively 'unjoined'.
    """
    # Joins are only meaningful for custom query logic layers (bizcore/serving).
    try:
      layer = getattr(getattr(self, "target_schema", None), "short_name", None)
      if layer not in ("bizcore", "serving"):
        return []
    except Exception:
      return []

    inputs = list(self._active_input_links_qs())
    if len(inputs) <= 1:
      return []

    joins_qs = self._active_joins_qs()
    try:
      join_count = joins_qs.count()
    except Exception:
      join_count = len(list(joins_qs))

    problems: list[str] = []

    # Case A: no joins at all
    if join_count == 0:
      # Treat first input as "base", all others need to be joined somehow.
      for link in inputs[1:]:
        src = getattr(link, "source_dataset", None)
        up = getattr(link, "upstream_target_dataset", None)
        role = (getattr(link, "role", "") or "").strip()
        name = ""
        if up is not None:
          name = getattr(up, "target_dataset_name", "") or str(up)
        elif src is not None:
          name = getattr(src, "dataset_name", "") or getattr(src, "name", "") or str(src)
        else:
          name = str(link)
        label = f"{name}"
        if role:
          label = f"{label} ({role})"
        problems.append(label)

      return problems

    # Case B: joins exist but some have no predicates (common “half configured” state)
    try:
      for j in joins_qs:
        pred_qs = getattr(j, "predicates", None)
        if pred_qs is None:
          continue
        pred_qs = pred_qs.all()
        if hasattr(pred_qs.model, "active"):
          pred_qs = pred_qs.filter(active=True)
        if pred_qs.count() == 0:
          problems.append(f"{str(j)} (no predicates)")
    except Exception:
      pass

    return problems

  @property
  def has_incomplete_joins(self) -> bool:
    return bool(self.missing_join_inputs)

  def build_natural_key_string(self, record_dict):
    """
    Build the concatenated string that represents the natural key,
    BEFORE pepper and hashing.

    Example output:
    "customer_id~4711 | mandant~100"
    """
    null_token = self.target_schema.surrogate_key_null_token  # e.g. "null_replaced"
    pair_sep = self.target_schema.surrogate_key_pair_separator
    comp_sep = f" {self.target_schema.surrogate_key_component_separator} "

    parts = []
    for field in self.natural_key_fields:
      value = record_dict.get(field, null_token)
      if value is None:
        value = null_token
      parts.append(f"{field}{pair_sep}{value}")
    # join components using ' | '
    return comp_sep.join(parts)
  
  def get_runtime_pepper(self):
    """
    Return the runtime pepper for deterministic surrogate key hashing.
    Priority:
    1. Environment variable 'ELEVATA_PEPPER'
    2. Profile-specific variable 'SEC_<PROFILE>_PEPPER'
    3. Django settings fallback
    """
    # 1. direct runtime override
    pepper = os.environ.get("ELEVATA_PEPPER")

    # 2. support profile-specific style (e.g. SEC_DEV_PEPPER)
    if not pepper:
      profile = os.environ.get("ELEVATA_PROFILE", "DEV").upper()
      env_key = f"SEC_{profile}_PEPPER"
      pepper = os.environ.get(env_key)

    # 3. fallback to settings
    if not pepper and hasattr(settings, "ELEVATA_PEPPER"):
      pepper = settings.ELEVATA_PEPPER

    if not pepper:
      raise ValueError(
        "No runtime pepper configured. Set ELEVATA_PEPPER or SEC_<PROFILE>_PEPPER in environment."
      )

    return pepper

  def preview_surrogate_key(self, record_dict):
    """
    Return a deterministic surrogate key (hash hex string) for a single logical record.
    This is for preview/diagnostics, not for bulk processing.
    """
    # Step 1: base natural key string (no pepper)
    base_key = self.build_natural_key_string(record_dict)

    # Step 2: add pepper
    pepper = self.get_runtime_pepper()
    key_with_pepper = f"{base_key}::{pepper}"

    # Step 3: hash using layer policy
    algo = self.target_schema.surrogate_key_algorithm  # e.g. "sha256"
    try:
      hash_func = getattr(hashlib, algo)
    except AttributeError:
      raise ValueError(f"Unsupported hash algorithm '{algo}'")

    return hash_func(key_with_pepper.encode("utf-8")).hexdigest()
  
  def save(self, *args, **kwargs):
    """
    Override save to keep surrogate key column names in sync with
    target_dataset_name for this dataset.

    If the dataset name changes, automatically rename all surrogate key
    columns in this dataset to "<target_dataset_name>_key".
    """
    # Automatically set retired_at when a row becomes inactive
    if not self.active and self.retired_at is None:
      self.retired_at = timezone.now()
    elif self.active:
      # If reactivated, clear retired_at
      self.retired_at = None

    old_name = None
    if self.pk:
      try:
        old = TargetDataset.objects.get(pk=self.pk)
        old_name = old.target_dataset_name
      except TargetDataset.DoesNotExist:
        old_name = None

    # normal save first
    super().save(*args, **kwargs)

    # if the dataset has been renamed, update surrogate key columns
    if old_name and old_name != self.target_dataset_name:
      # build new surrogate key name
      new_sk_name = naming.build_surrogate_key_name(self.target_dataset_name)

      # Update all surrogate key columns of this dataset
      TargetColumn.objects.filter(
        target_dataset=self,
        system_role="surrogate_key",
      ).update(target_column_name=new_sk_name)

      # If this dataset is referenced by other datasets, their FK column names depend on
      # the parent dataset name. Re-sync all inbound references so planner can rename FK columns.
      for ref in self.incoming_references.select_related("referencing_dataset").all():
        try:
          with transaction.atomic():
            ref.sync_child_fk_column()
        except Exception:
          pass


# -------------------------------------------------------------------
# QueryNode
# -------------------------------------------------------------------
class QueryNode(AuditFields):
  """
  Query tree node owned by a single TargetDataset.
  Base/root is referenced from TargetDataset.query_root; the current leaf/head is TargetDataset.query_head.  
  """
  target_dataset = models.ForeignKey("TargetDataset", on_delete=models.CASCADE, related_name="query_nodes",
    help_text="Owning dataset (lifecycle/permissions scope).",
  )
  node_type = models.CharField(max_length=16, choices=QueryNodeType.choices,
    help_text="Type of query operator represented by this node.",
  )
  name = models.CharField(max_length=128, blank=True, default="",
    help_text="Optional label for UI (e.g. 'Base Select', 'Agg: Daily', 'Union: Sources').",
  )
  active = models.BooleanField(default=True,
    help_text="Enable/disable this query node. Disabled nodes are ignored by the builder.",
  )

  class Meta:
    db_table = "query_node"
    indexes = [
      models.Index(fields=["target_dataset", "node_type"]),
    ]

  def __str__(self) -> str:
    """
    Human-friendly label for dropdowns/UI:
    include target dataset name so users don't have to know node IDs.
    """
    try:
      td = getattr(self, "target_dataset", None)
      td_name = getattr(td, "target_dataset_name", None) or "?"
    except Exception:
      td_name = "?"

    nt = (getattr(self, "node_type", "") or "").strip() or "node"
    nm = (getattr(self, "name", "") or "").strip()

    # Example: "bc_dim_customer_other · union · UNION (node#25)"
    label = f"{td_name} · {nt}"
    if nm:
      label += f" · {nm}"
    label += f" (node#{self.pk})"
    return label


# -------------------------------------------------------------------
# QuerySelectNode
# -------------------------------------------------------------------
class QuerySelectNode(AuditFields):
  node = models.OneToOneField(QueryNode, on_delete=models.CASCADE, related_name="select",
    limit_choices_to={"node_type": QueryNodeType.SELECT},
    help_text="Query node header for this SELECT operator.",  
  )
  # Optional: in future allow a select-node to reference a *different* dataset definition
  # For now: always use node.target_dataset
  use_dataset_definition = models.BooleanField(default=True,
    help_text="When true, build select from owning TargetDataset definition (joins/columns/manual expressions).",
  )

  class Meta:
    db_table = "query_select_node"
    verbose_name = "Query select node"
    verbose_name_plural = "Query select nodes"

  def __str__(self) -> str:
    node = getattr(self, "node", None)
    if node is not None:
      return str(node)
    return f"select#{self.pk}"


# -------------------------------------------------------------------
# QueryAggregateNode
# -------------------------------------------------------------------
class QueryAggregateNode(AuditFields):
  node = models.OneToOneField(QueryNode, on_delete=models.CASCADE, related_name="aggregate",
    limit_choices_to={"node_type": QueryNodeType.AGGREGATE},
    help_text="Query node header for this AGGREGATE operator.",
  )
  input_node = models.ForeignKey(QueryNode, on_delete=models.PROTECT, related_name="used_as_aggregate_input",
    help_text="Input query node providing rows to aggregate (wrapped as subquery).",
  )
  mode = models.CharField(max_length=16, choices=AGGREGATE_MODE_CHOICES, default="grouped",
    help_text="Grouped requires group keys; Global allows measures without group keys.",
  )

  class Meta:
    db_table = "query_aggregate_node"
    verbose_name = "Query aggregate node"
    verbose_name_plural = "Query aggregate nodes"

  def __str__(self) -> str:
    node = getattr(self, "node", None)
    if node is not None:
      return str(node)
    return f"aggregate#{self.pk}"


# -------------------------------------------------------------------
# QueryAggregateNodeGroupKey
# -------------------------------------------------------------------
class QueryAggregateGroupKey(AuditFields):
  aggregate_node = models.ForeignKey(QueryAggregateNode, on_delete=models.CASCADE, related_name="group_keys",
    help_text="Aggregate operator this group key belongs to.",
  )
  # Start minimal: reference by column name from the input projection.
  # Later: FK to a QueryExpression table.
  input_column_name = models.CharField(max_length=255,
    help_text="Column name exposed by input node to group by.",
  )
  output_name = models.CharField(max_length=255, blank=True, default="",
    help_text="Optional alias in the aggregate output (defaults to input_column_name).",
  )
  ordinal_position = models.PositiveIntegerField(default=0,
    help_text="Ordering of group keys in UI and generated SQL.",
  )

  class Meta:
    db_table = "query_aggregate_node_group_key"
    unique_together = [("aggregate_node", "ordinal_position")]

  def __str__(self) -> str:
    col = (getattr(self, "input_column_name", "") or "").strip()
    out = (getattr(self, "output_name", "") or "").strip()
    ordpos = getattr(self, "ordinal_position", None)
    bits = []
    if ordpos is not None:
      bits.append(f"#{ordpos}")
    if col:
      bits.append(col)
    if out and out != col:
      bits.append(f"→ {out}")
    return " ".join(bits) if bits else f"group_key#{self.pk}"


# -------------------------------------------------------------------
# QueryAggregateMeasure
# -------------------------------------------------------------------
class QueryAggregateMeasure(AuditFields):
  aggregate_node = models.ForeignKey(QueryAggregateNode, on_delete=models.CASCADE, related_name="measures",
    help_text="Aggregate operator this measure belongs to.",
  )
  output_name = models.CharField(max_length=255,
    help_text="Output column name (can be friendly in serving).",
  )
  function = models.CharField(max_length=32, choices=AggregateFunction.choices,
    help_text="Aggregate function.",
  )
  # Minimal: one arg column.
  # Later: allow expression args via QueryExpression / AST serialization.
  input_column_name = models.CharField(max_length=255, blank=True, default="",
    help_text="Input column used as argument (empty for COUNT(*)).",
  )
  delimiter = models.CharField(max_length=64, blank=True, default=",",
    help_text="Delimiter used by STRING_AGG (ignored for other aggregate functions).",
  )
  order_by = models.ForeignKey("OrderByExpression", null=True, blank=True, on_delete=models.PROTECT,
    help_text="Optional ORDER BY expression used inside the aggregate function (e.g., STRING_AGG).",
  )  
  distinct = models.BooleanField(default=False,
    help_text="If true, applies DISTINCT on the argument when supported (e.g., COUNT(DISTINCT x)).",
  )
  ordinal_position = models.PositiveIntegerField(default=0,
    help_text="Ordering of measures in UI and generated SQL.",
  )

  class Meta:
    db_table = "query_aggregate_measure"
    unique_together = [("aggregate_node", "ordinal_position")]

  def __str__(self) -> str:
    out = (getattr(self, "output_name", "") or "").strip()
    fn = (getattr(self, "function", "") or "").strip()
    col = (getattr(self, "input_column_name", "") or "").strip()
    ordpos = getattr(self, "ordinal_position", None)
    sig = fn
    if col:
      sig = f"{fn}({col})"
    elif fn.upper() == "COUNT":
      sig = "COUNT(*)"
    bits = []
    if ordpos is not None:
      bits.append(f"#{ordpos}")
    if out:
      bits.append(out)
    if sig:
      bits.append(sig)
    return " · ".join(bits) if bits else f"measure#{self.pk}"

# -------------------------------------------------------------------
# OrderByExpression
# -------------------------------------------------------------------
class OrderByExpression(AuditFields):
  """
  Reusable ORDER BY definition (multi-key) for aggregates/windows.
  Scoped to a TargetDataset so validation/UX stays intuitive.
  """
  target_dataset = models.ForeignKey("TargetDataset", on_delete=models.CASCADE, related_name="order_by_expressions",
    help_text="Owning dataset (permissions / lifecycle scope).",
  )
  name = models.CharField(max_length=128,
    help_text="Label used in the UI (e.g. 'By Date Desc', 'By Customer, Date').",
  )
  active = models.BooleanField(default=True,
    help_text="Whether this ORDER BY definition is active and used by referencing expressions.",
  )

  class Meta:
    db_table = "order_by_expression"
    unique_together = [("target_dataset", "name")]
    indexes = [
      models.Index(fields=["target_dataset", "active"]),
    ]

  def __str__(self) -> str:
    return (getattr(self, "name", "") or "").strip() or f"orderby#{self.pk}"


# -------------------------------------------------------------------
# OrderByItem
# -------------------------------------------------------------------
class OrderByItem(AuditFields):
  """
  One sort key of an OrderByExpression.
  References columns by name from the *input projection*.
  """
  order_by = models.ForeignKey(OrderByExpression, on_delete=models.CASCADE, related_name="items",
    help_text="ORDER BY definition this item belongs to.",
  )
  input_column_name = models.CharField(max_length=255,
    help_text="Column name exposed by the input node to order by.",
  )
  direction = models.CharField(max_length=4, choices=ORDER_BY_DIR_CHOICES, default="ASC",
    help_text="Sort direction.",
  )
  nulls_placement = models.CharField(max_length=5, choices=NULLS_PLACEMENT_CHOICES, blank=True, default="",
    help_text="NULLS placement if supported by dialect.",
  )
  ordinal_position = models.PositiveIntegerField(default=0,
    help_text="Position of this item inside the ORDER BY list (0..n).",
  )

  class Meta:
    db_table = "order_by_item"
    unique_together = [("order_by", "ordinal_position")]

  def __str__(self) -> str:
    col = (getattr(self, "input_column_name", "") or "").strip()
    direction = (getattr(self, "direction", "") or "").strip() or "ASC"
    nulls = (getattr(self, "nulls_placement", "") or "").strip()
    ordpos = getattr(self, "ordinal_position", None)
    bits = []
    if ordpos is not None:
      bits.append(f"#{ordpos}")
    if col:
      bits.append(f"{col} {direction}")
    else:
      bits.append(direction)
    if nulls:
      bits.append(f"NULLS {nulls}")
    return " ".join(bits) if bits else f"order_item#{self.pk}"


# -------------------------------------------------------------------
# QueryUnionNode
# -------------------------------------------------------------------
class QueryUnionNode(AuditFields):
  node = models.OneToOneField(QueryNode, on_delete=models.CASCADE, related_name="union",
    limit_choices_to={"node_type": QueryNodeType.UNION},
    help_text="Query node header for this UNION operator.",
  )
  mode = models.CharField(max_length=16, choices=UnionMode.choices, default=UnionMode.UNION_ALL,
    help_text="UNION removes duplicates, UNION ALL keeps duplicates (faster).",
  )

  class Meta:
    db_table = "query_union_node"
    verbose_name = "Query union node"
    verbose_name_plural = "Query union nodes"

  def __str__(self) -> str:
    node = getattr(self, "node", None)
    if node is not None:
      return str(node)
    return f"union#{self.pk}"


# -------------------------------------------------------------------
# QueryUnionOutputColumn
# -------------------------------------------------------------------
class QueryUnionOutputColumn(AuditFields):
  union_node = models.ForeignKey(QueryUnionNode, on_delete=models.CASCADE, related_name="output_columns",
    help_text="Union operator this output column belongs to (schema contract).",
  )
  output_name = models.CharField(max_length=255,
    help_text="Output column name of the UNION schema contract.",
  )
  ordinal_position = models.PositiveIntegerField(default=0,
    help_text="Ordering of output columns in the UNION schema contract.",
  )
  datatype = models.CharField(max_length=20, choices=DATATYPE_CHOICES, blank=True, null=True,
    help_text="Optional logical datatype of the UNION output column.",
  )
  max_length = models.IntegerField(blank=True, null=True,
    help_text="Optional length parameter (e.g., VARCHAR(100))."
  )
  decimal_precision = models.IntegerField(blank=True, null=True,
    help_text="Optional precision for DECIMAL/NUMERIC (e.g., DECIMAL(18,2))."
  )
  decimal_scale = models.IntegerField(blank=True, null=True,
    help_text="Optional scale for DECIMAL/NUMERIC (e.g., DECIMAL(18,2))."
  )

  class Meta:
    db_table = "query_union_output_column"
    unique_together = [("union_node", "ordinal_position")]

  def __str__(self) -> str:
    name = (getattr(self, "output_name", "") or "").strip()
    dt = getattr(self, "datatype", None)
    ordpos = getattr(self, "ordinal_position", None)
    bits = []
    if ordpos is not None:
      bits.append(f"#{ordpos}")
    if name:
      bits.append(name)
    if dt:
      bits.append(f": {dt}")
    return " ".join(bits) if bits else f"union_col#{self.pk}"

  def clean(self):
    super().clean()

    # If datatype is set, validate required parameters for common types.
    dt = (self.datatype or "").strip().lower()
    if not dt:
      return

    # Very lightweight normalization (avoid dialect-specific overreach).
    is_decimal = dt.startswith("decimal") or dt.startswith("numeric")
    is_varchar = dt.startswith("varchar") or dt.startswith("char") or dt.startswith("nvarchar") or dt.startswith("nchar")

    if is_decimal:
      if self.decimal_precision is None or self.decimal_scale is None:
        raise ValidationError({
          "decimal_precision": "Required when datatype is DECIMAL/NUMERIC.",
          "decimal_scale": "Required when datatype is DECIMAL/NUMERIC.",
        })
      if self.decimal_precision is not None and self.decimal_precision <= 0:
        raise ValidationError({"decimal_precision": "Must be > 0."})
      if self.decimal_scale is not None and self.decimal_scale < 0:
        raise ValidationError({"decimal_scale": "Must be >= 0."})
      if (self.decimal_precision is not None and self.decimal_scale is not None
          and self.decimal_scale > self.decimal_precision):
        raise ValidationError({"decimal_scale": "Scale must be <= precision."})

    if is_varchar:
      if self.max_length is None:
        raise ValidationError({"max_length": "Required when datatype is a character type (VARCHAR/CHAR/...)." })
      if self.max_length is not None and self.max_length <= 0:
        raise ValidationError({"max_length": "Must be > 0."})



# -------------------------------------------------------------------
# QueryUnionBranch
# -------------------------------------------------------------------
class QueryUnionBranch(AuditFields):
  union_node = models.ForeignKey(QueryUnionNode, on_delete=models.CASCADE, related_name="branches",
    help_text="Union operator this branch belongs to.",
  )
  input_node = models.ForeignKey(QueryNode, on_delete=models.PROTECT, related_name="used_as_union_branch",
    help_text="Branch query node producing rows to union.",
  )
  ordinal_position = models.PositiveIntegerField(default=0,
    help_text="Ordering of UNION branches in UI and generated SQL.",
  )

  class Meta:
    db_table = "query_union_branch"
    unique_together = [("union_node", "ordinal_position")]

  def __str__(self) -> str:
    # Human-friendly label used in dropdowns and lists.
    node = getattr(self, "input_node", None)
    node_name = getattr(node, "name", None) or getattr(node, "node_type", None) or "node"
    td = getattr(node, "target_dataset", None)
    td_name = getattr(td, "target_dataset_name", None) or ""
    ord_pos = getattr(self, "ordinal_position", None)
    prefix = f"Branch #{ord_pos}" if ord_pos is not None else "Branch"
    if td_name:
      return f"{prefix}: {node_name} · {td_name}"
    return f"{prefix}: {node_name}"


# -------------------------------------------------------------------
# QueryUnionBranchMapping
# -------------------------------------------------------------------
class QueryUnionBranchMapping(AuditFields):
  branch = models.ForeignKey(QueryUnionBranch, on_delete=models.CASCADE, related_name="mappings",
    help_text="Branch this mapping belongs to.",
  )
  output_column = models.ForeignKey(QueryUnionOutputColumn, on_delete=models.CASCADE, related_name="branch_mappings",
    help_text="Output column of the UNION schema contract that is being populated.",
  )
  # Minimal: map by input column name from branch projection.
  # Later: allow expressions/casts via QueryExpression.
  input_column_name = models.CharField(max_length=255,
    help_text="Column name exposed by branch input node.",
  )

  class Meta:
    db_table = "query_union_branch_mapping"
    unique_together = [("branch", "output_column")]

  def __str__(self) -> str:
    out = getattr(self, "output_column", None)
    out_name = (getattr(out, "output_name", "") or "").strip() if out is not None else ""
    inp = (getattr(self, "input_column_name", "") or "").strip()
    if out_name and inp:
      return f"{out_name} ← {inp}"
    return out_name or inp or f"mapping#{self.pk}"


# -------------------------------------------------------------------
# QueryWindowNode
# -------------------------------------------------------------------
class QueryWindowNode(AuditFields):
  node = models.OneToOneField(QueryNode, on_delete=models.CASCADE, related_name="window",
    limit_choices_to={"node_type": QueryNodeType.WINDOW},
    help_text="Query node holding window function definitions.",
  )
  input_node = models.ForeignKey(QueryNode, on_delete=models.PROTECT, related_name="used_as_window_input",
    help_text="Input query node providing rows for window functions (wrapped as subquery).",
  )

  class Meta:
    db_table = "query_window_node"
    verbose_name = "Query window node"
    verbose_name_plural = "Query window nodes"

  def __str__(self) -> str:
    node = getattr(self, "node", None)
    if node is not None:
      return str(node)
    return f"window#{self.pk}"


# -------------------------------------------------------------------
# PartitionByExpression
# -------------------------------------------------------------------
class PartitionByExpression(AuditFields):
  target_dataset = models.ForeignKey("TargetDataset", on_delete=models.CASCADE, related_name="partition_by_expressions",
    help_text="Owning dataset (permissions / lifecycle scope).",
  )
  name = models.CharField(max_length=128,
    help_text="Label used in the UI (e.g. 'By Customer', 'By Customer + Day').",
  )
  active = models.BooleanField(default=True,
    help_text="Controls whether this PARTITION BY definition can be selected and used in queries.",
  )

  class Meta:
    db_table = "partition_by_expression"
    unique_together = [("target_dataset", "name")]
    indexes = [models.Index(fields=["target_dataset", "active"])]

  def __str__(self) -> str:
    name = (getattr(self, "name", "") or "").strip()
    return name or f"partition_by#{self.pk}"


# -------------------------------------------------------------------
# PartitionByItem
# -------------------------------------------------------------------
class PartitionByItem(AuditFields):
  partition_by = models.ForeignKey(PartitionByExpression, on_delete=models.CASCADE, related_name="items",
    help_text="PARTITION BY definition this item belongs to.",
  )
  input_column_name = models.CharField(max_length=255,
    help_text="Column name exposed by the input node to partition by.",
  )
  ordinal_position = models.PositiveIntegerField(default=0,
    help_text="Position of this partition key within the PARTITION BY clause.",
  )

  class Meta:
    db_table = "partition_by_item"
    unique_together = [("partition_by", "ordinal_position")]

  def __str__(self) -> str:
    col = (getattr(self, "input_column_name", "") or "").strip()
    ordpos = getattr(self, "ordinal_position", None)
    if ordpos is not None and col:
      return f"#{ordpos} {col}"
    return col or f"partition_item#{self.pk}"


# -------------------------------------------------------------------
# QueryWindowColumn
# -------------------------------------------------------------------
class QueryWindowColumn(AuditFields):
  window_node = models.ForeignKey(QueryWindowNode, on_delete=models.CASCADE, related_name="columns",
    help_text="Window operator this output column belongs to.",
  )
  output_name = models.CharField(max_length=255,
    help_text="Output column name (can be friendly in serving).",
  )
  function = models.CharField(max_length=32, choices=WINDOW_FUNCTION_CHOICES,
    help_text="Window function.",
  )
  partition_by = models.ForeignKey(PartitionByExpression, null=True, blank=True, on_delete=models.PROTECT,
    help_text="Optional PARTITION BY definition.",
  )
  order_by = models.ForeignKey(OrderByExpression, null=True, blank=True, on_delete=models.PROTECT,
    help_text="Optional ORDER BY definition (recommended for deterministic results).",
  )
  ordinal_position = models.PositiveIntegerField(default=0,
    help_text="Position of this window output column in the projection.",
  )
  active = models.BooleanField(default=True,
    help_text="Whether this window output column is enabled (included in SQL rendering).",
  )

  class Meta:
    db_table = "query_window_column"
    unique_together = [("window_node", "ordinal_position")]

  def __str__(self) -> str:
    out = (getattr(self, "output_name", "") or "").strip()
    fn = (getattr(self, "function", "") or "").strip()
    ordpos = getattr(self, "ordinal_position", None)
    if ordpos is not None and out and fn:
      return f"#{ordpos} {out} · {fn}"
    if out and fn:
      return f"{out} · {fn}"
    return out or fn or f"window_col#{self.pk}"


class QueryWindowColumnArg(AuditFields):
  """
  Ordered arguments for window functions (e.g. NTILE(4), LAG(col, 1, 'n/a')).
  Stored in a normalized form so the UI can evolve without schema changes.
  """
  window_column = models.ForeignKey(QueryWindowColumn,  on_delete=models.CASCADE, related_name="args",
    help_text="Window column this argument belongs to.",
  )
  arg_type = models.CharField(max_length=8, choices=WINDOW_ARG_TYPE_CHOICES,
    help_text="Argument type.",
  )
  column_name = models.CharField(max_length=255, blank=True, default="",
    help_text="Input column name (required when arg_type='column').",
  )
  int_value = models.IntegerField(null=True, blank=True,
    help_text="Integer literal (required when arg_type='int').",
  )
  str_value = models.CharField(max_length=255, blank=True, default="",
    help_text="String literal (required when arg_type='str').",
  )
  ordinal_position = models.PositiveIntegerField(default=0,
    help_text="Position of this argument in the function call.",
  )

  class Meta:
    db_table = "query_window_column_arg"
    unique_together = [("window_column", "ordinal_position")]

  def __str__(self) -> str:
    arg_type = (getattr(self, "arg_type", "") or "").strip()
    col = (getattr(self, "column_name", "") or "").strip()
    ival = getattr(self, "int_value", None)
    sval = (getattr(self, "str_value", "") or "").strip()
    ordpos = getattr(self, "ordinal_position", None)

    value = ""
    if arg_type == "column":
      value = col
    elif arg_type == "int" and ival is not None:
      value = str(ival)
    elif arg_type == "str" and sval:
      value = f"'{sval}'"
    else:
      value = col or (str(ival) if ival is not None else "") or (f"'{sval}'" if sval else "")

    if ordpos is not None and value:
      return f"#{ordpos}: {value}"
    return value or f"arg#{self.pk}"


# -------------------------------------------------------------------
# TargetDatasetInput
# -------------------------------------------------------------------
class TargetDatasetInput(AuditFields):
  target_dataset = models.ForeignKey(TargetDataset, on_delete=models.CASCADE, related_name="input_links",
    help_text="The stage/rawcore/bizcore dataset being built."
  )
  source_dataset = models.ForeignKey(SourceDataset, null=True, blank=True, related_name="output_links", on_delete=models.PROTECT, 
    help_text="The source dataset contributing to this target dataset."
  )
  upstream_target_dataset = models.ForeignKey(TargetDataset, null=True, blank=True, related_name="downstream_input_links", on_delete=models.PROTECT,
    help_text="If set, this target dataset is used as upstream instead of a source dataset."
  )
  role = models.CharField(max_length=50, choices=TARGET_DATASET_INPUT_ROLE_CHOICES,
    help_text=(
      "How this source contributes to the target: "
      "primary (golden source), enrichment (same entity extra attrs), "
      "reference_lookup (dim/code join), or audit_only (technical metadata)."
    ),
  )
  active = models.BooleanField(default=True,
    help_text="If unchecked, this mapping is retained for lineage/audit but is no longer used for load."
  )

  class Meta:
    db_table = "target_dataset_input"
    constraints = [
      models.UniqueConstraint(
        fields=["target_dataset", "source_dataset", "upstream_target_dataset"],
        name="unique_source_per_target_dataset",
      ),
      models.CheckConstraint(
        name="td_input_exactly_one_upstream",
        condition=(
          models.Q(source_dataset__isnull=False, upstream_target_dataset__isnull=True) |
          models.Q(source_dataset__isnull=True, upstream_target_dataset__isnull=False)
        ),
      )
    ]

  def __str__(self):
    """
    Human-readable representation of this dataset-level lineage.

    - If we have a source_dataset, show that.
    - Otherwise, if we have an upstream_target_dataset, show that.
    - Otherwise, show a dash.
    """
    if getattr(self, "source_dataset_id", None):
      src_label = str(self.source_dataset)
    elif hasattr(self, "upstream_target_dataset") and getattr(self, "upstream_target_dataset_id", None):
      src_label = str(self.upstream_target_dataset)
    else:
      src_label = "—"

    role = getattr(self, "role", "") or ""
    role_part = f"{role} · " if role else ""
    return f"{role_part}{src_label} -> {self.target_dataset}"

# -------------------------------------------------------------------
# TargetDatasetJoin
# -------------------------------------------------------------------
class TargetDatasetJoin(AuditFields):
  """
  Represents an explicit JOIN edge between two TargetDatasetInputs of the same
  TargetDataset (typically for Bizcore/Serving multi-upstream enrichment).
  """
  target_dataset = models.ForeignKey("TargetDataset", on_delete=models.CASCADE, related_name="joins",
    help_text=(
      "The dataset that will be built (Bizcore/Serving). "
      "This join defines how multiple upstream inputs are combined."
    ),
  )
  left_input = models.ForeignKey("TargetDatasetInput", on_delete=models.CASCADE, related_name="as_left_join",
    help_text=(
      "Left side of the join (primary input). "
      "This is typically the 'main entity' input (e.g. person). "
      "In generated SQL, this becomes the main FROM source."
    ),
  )
  right_input = models.ForeignKey("TargetDatasetInput", on_delete=models.CASCADE, related_name="as_right_join",
    help_text=(
      "Right side of the join (enrichment / lookup input). "
      "In generated SQL, this becomes the JOINed source."
    ),
  )
  join_type = models.CharField(max_length=10, choices=JOIN_TYPE_CHOICES, default="left",
    help_text=(
      "Join type.\n"
      "Typical patterns:\n"
      "  - LEFT: keep all rows from the left input and enrich from the right\n"
      "  - INNER: only rows that match on the join condition\n"
      "  - CROSS: no condition, creates a Cartesian product (date spine etc.)\n\n"
      "Note: CROSS JOIN must not have predicates."
    ),
  )
  join_order = models.PositiveIntegerField(default=1,
    help_text=(
      "Deterministic ordering when multiple joins exist. "
      "If you join multiple inputs (A join B join C), elevata applies joins in this order."
    ),
  )
  description = models.TextField(blank=True, default="",
    help_text=(
      "Explain the business intent of this join.\n"
      "Example: 'Enrich customers with their latest known address.'\n"
      "This text can be shown in details/snapshots for explainability."
    ),
  )

  class Meta:
    ordering = ["join_order", "id"]
    constraints = [
      models.UniqueConstraint(
        fields=["target_dataset", "join_order"],
        name="uniq_targetdataset_join_order",
      ),
      models.UniqueConstraint(
        fields=["target_dataset", "left_input", "right_input"],
        name="uniq_targetdataset_join_edge",
      ),
    ]

  @property
  def has_missing_predicates(self) -> bool:
    """
    True if this join requires ON predicates but none are defined.
    CROSS joins do not require predicates.
    """
    join_type = (getattr(self, "join_type", "") or "").lower().strip()
    if join_type == "cross":
      return False

    preds = getattr(self, "predicates", None)
    if preds is None:
      # be conservative: if we cannot inspect predicates, do not claim it's missing
      return False

    qs = preds.all()
    # if Predicate model has active flag, respect it
    try:
      if qs.model and hasattr(qs.model, "active"):
        qs = qs.filter(active=True)
    except Exception:
      pass

    try:
      return not qs.exists()
    except Exception:
      return len(list(qs)) == 0

  def clean(self):
    # Ensure both inputs belong to the same target_dataset.
    if self.left_input_id and self.left_input.target_dataset_id != self.target_dataset_id:
      raise ValidationError({"left_input": "left_input must belong to the same target_dataset."})

    if self.right_input_id and self.right_input.target_dataset_id != self.target_dataset_id:
      raise ValidationError({"right_input": "right_input must belong to the same target_dataset."})

    # Prevent self-joins on the same input.
    if self.left_input_id and self.right_input_id and self.left_input_id == self.right_input_id:
      raise ValidationError({"right_input": "right_input must differ from left_input."})

    # CROSS join must not have predicates (enforced in predicate.clean too, but nice to keep here).
    # Note: At clean() time predicates might not be saved yet; builder should enforce as well.
    return super().clean()

  def __str__(self):
    def _upstream_name(inp):
      # For TargetDatasetInput: prefer upstream_target_dataset, then source_dataset
      utd = getattr(inp, "upstream_target_dataset", None)
      if utd is not None:
        return utd.target_dataset_name
      sd = getattr(inp, "source_dataset", None)
      if sd is not None:
        return sd.source_dataset_name
      # Fallback: if __str__ is "X -> Y", take left part
      s = str(inp)
      if "->" in s:
        return s.split("->", 1)[0].strip()
      return s

    left = _upstream_name(self.left_input) if self.left_input_id else "?"
    right = _upstream_name(self.right_input) if self.right_input_id else "?"
    return f"{self.join_type} join ({left} -> {right})"


# -------------------------------------------------------------------
# TargetDatasetJoinPredicate
# -------------------------------------------------------------------
class TargetDatasetJoinPredicate(AuditFields):
  """
  Structured join predicate to support intuitive authoring without requiring
  deep SQL knowledge.

  Expressions may be:
    - column references (e.g. country_code)
    - constants (e.g. 'DE', 1)
    - DSL blocks (e.g. {{ lower(country_code) }})
    - function expressions (e.g. lower(country_code))
  """
  join = models.ForeignKey("TargetDatasetJoin", on_delete=models.CASCADE, related_name="predicates",
  )
  ordinal_position = models.PositiveIntegerField(default=1,
    help_text=(
      "Predicate ordering. Predicates are combined with AND in this order (MVP).\n"
      "Example: 1) id equality, 2) valid_from <= date, 3) date < valid_to."
    ),
  )
  left_expr = models.TextField(
    help_text=(
      "Left-side expression.\n"
      "You can enter:\n"
      "  - a column name (recommended): address_id\n"
      "  - a constant: 'DE'\n"
      "  - a simple expression: lower(country_code)\n"
      "  - DSL block: {{ coalesce(country_code, 'DE') }}\n\n"
      "Tip: Usually you do NOT need to add table aliases. elevata can qualify references "
      "based on your configured upstream inputs."
    ),
  )
  operator = models.CharField(max_length=20, choices=OPERATOR_CHOICES, default="=",
    help_text=(
      "Comparison operator.\n"
      "Rules:\n"
      "  - BETWEEN uses right_expr (lower) + right_expr_2 (upper)\n"
      "  - IS NULL / IS NOT NULL do not use right_expr\n"
      "  - All other operators use right_expr\n\n"
      "Examples:\n"
      "  - customer_id = person_id\n"
      "  - country_code = 'DE'\n"
      "  - valid_from <= d.date\n"
      "  - d.date BETWEEN valid_from AND valid_to"
    ),
  )
  right_expr = models.TextField(blank=True, null=True,
    help_text=(
      "Right-side expression (required for most operators).\n"
      "Examples:\n"
      "  - a column name: person_id\n"
      "  - a constant: 'DE', 1\n"
      "  - a function call: date_trunc('day', ts)\n"
      "  - DSL block: {{ date_trunc('day', ts) }}\n"
    ),
  )
  right_expr_2 = models.TextField(blank=True, null=True,
    help_text=(
      "Second right-side expression (only for BETWEEN).\n"
      "Example:\n"
      "  d.date BETWEEN valid_from AND valid_to"
    ),
  )

  class Meta:
    ordering = ["ordinal_position", "id"]
    constraints = [
      models.UniqueConstraint(
        fields=["join", "ordinal_position"],
        name="uniq_join_predicate_order",
      ),
    ]

  def clean(self):
    # CROSS joins must not have predicates.
    if self.join_id and self.join.join_type == "cross":
      raise ValidationError("CROSS JOIN must not define predicates.")

    op = self.operator

    unary_ops = {"IS NULL", "IS NOT NULL"}
    between_ops = {"BETWEEN"}
    binary_ops = {
      "=", "!=", "<", "<=", ">", ">=", "LIKE", "IN"
    }

    if op in unary_ops:
      if self.right_expr or self.right_expr_2:
        raise ValidationError({"right_expr": "Unary operator must not have right_expr."})

    if op in binary_ops:
      if not (self.right_expr and str(self.right_expr).strip()):
        raise ValidationError({"right_expr": "Binary operator requires right_expr."})
      if self.right_expr_2:
        raise ValidationError({"right_expr_2": "Binary operator must not have right_expr_2."})

    if op in between_ops:
      if not (self.right_expr and str(self.right_expr).strip()):
        raise ValidationError({"right_expr": "BETWEEN requires right_expr (lower bound)."})
      if not (self.right_expr_2 and str(self.right_expr_2).strip()):
        raise ValidationError({"right_expr_2": "BETWEEN requires right_expr_2 (upper bound)."})

    return super().clean()

  def __str__(self):
    return f"{self.join_id}: {self.left_expr} {self.operator} {self.right_expr or ''}".strip()


# -------------------------------------------------------------------
# TargetDatasetOwnership
# -------------------------------------------------------------------
class TargetDatasetOwnership(AuditFields):
  target_dataset = models.ForeignKey("TargetDataset", on_delete=models.CASCADE, related_name="target_dataset_ownerships",
    help_text="The dataset for which an owner is declared."
  )
  person = models.ForeignKey("Person", on_delete=models.PROTECT, related_name="target_dataset_ownerships",
    help_text="The person who has the ownership for the dataset."
  )
  role = models.CharField(max_length=20, choices=ROLE_CHOICES,
    help_text="The role which the person has on the dataset."
  )
  is_primary_owner = models.BooleanField(default=False,
    help_text="If checked, this is the primary ownership."
  )
  since = models.DateField(blank=True, null=True,
    help_text="The date from which the ownership will start."
  )
  until = models.DateField(blank=True, null=True,
    help_text="The date on which the ownership will end."
  )
  remark = models.CharField(max_length=255, blank=True, null=True,
    help_text="Optional remarks concerning the ownership."
  )

  class Meta:
    constraints = [models.UniqueConstraint(fields=["target_dataset", "person", "role"], name="unique_target_dataset_ownership")]
    ordering = ["-is_primary_owner", "target_dataset", "role", "since"]

  def __str__(self):
    return f"{self.target_dataset} · {self.person} ({self.role})"

# -------------------------------------------------------------------
# TargetColumn
# -------------------------------------------------------------------
class TargetColumn(AuditFields):
  target_dataset = models.ForeignKey(TargetDataset, on_delete=models.CASCADE, related_name="target_columns",
    help_text="The dataset this column belongs to."
  )
  # NOTE: Naming rules are enforced in forms.py (schema-dependent) and in health/validators.
  # Model field validators are static and cannot depend on target_schema.
  target_column_name = models.CharField(max_length=63,
    help_text="Final column name in snake_case. eg. 'customer_name', 'order_created_tms'."
  )
  ordinal_position = models.PositiveIntegerField(
    help_text="Column order within the dataset."
  )
  source_columns = models.ManyToManyField("SourceColumn", through="TargetColumnInput", 
    through_fields=("target_column", "source_column"), related_name="mapped_target_columns", blank=True,
    help_text="Which source columns contribute to this target column."
  )
  upstream_columns = models.ManyToManyField("self", through="TargetColumnInput",
    through_fields=("target_column", "upstream_target_column"), symmetrical=False,
    related_name="downstream_columns", blank=True,
    help_text=(
      "Which other target columns feed this target column as upstream "
      "instead of source columns."
    )
  )
  datatype = models.CharField(max_length=20, choices=DATATYPE_CHOICES,
    help_text="Logical / normalized datatype."
  )
  max_length = models.PositiveIntegerField(blank=True, null=True,
    help_text="How many characters the field may have."
  )
  decimal_precision = models.PositiveIntegerField(blank=True, null=True,
    help_text="If datatype decimal The decimal precision of the column."
  )
  decimal_scale = models.PositiveIntegerField(blank=True, null=True,
    help_text="If datatype decimal, the number of decimal places of the column."
  )
  nullable = models.BooleanField(default=True,
    help_text="Whether this column can be NULL in the final target dataset."
  )

  # NOTE:
  # system_role is the single source of truth for all system-managed columns
  # (surrogate keys, entity keys, row_hash, technical and versioning columns).
  # Do not infer semantics from naming conventions.
  system_role = models.CharField(max_length=30, choices=SYSTEM_COLUMN_ROLE_CHOICES, blank=True, default="",
    help_text=(
      "Semantic role for system-managed columns. "
      "Used to reliably render/execute technical columns (e.g. load_run_id, loaded_at) "
      "and to support forensics without relying on naming conventions."
    ),
  )
  manual_expression = models.TextField(blank=True, null=True,
    help_text=(
      "Optional expression for deriving this column.\n"
      "- If you use elevata DSL, wrap it in {{ ... }}, e.g. {{ UPPER(customer_name) }}.\n"
      "- If you enter plain SQL without {{ }}, it is treated as target-specific SQL and "
      "used directly in generated SQL / SQL preview.\n"
      "This default applies to all inputs unless overridden at source level."
    )
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Business description / semantic meaning of the column."
  )
  pii_level = models.CharField(max_length=30, choices=PII_LEVEL_CHOICES, default="none",
    help_text="PII classification of this column, if applicable."
  )
  remark = models.CharField(max_length=255, blank=True, null=True,
    help_text="Free-form notes."
  )
  sensitivity = models.CharField(max_length=30, choices=SENSITIVITY_CHOICES, default="public",
    help_text="Sensitivity classification for this column (e.g. public, confidential, restricted)."
  )
  lineage_origin = models.CharField(max_length=20, choices=LINEAGE_ORIGIN_CHOICES, default="direct",
    help_text="How this column is derived: direct source field, derived calculation, lookup join, constant, etc."
  )
  surrogate_expression = models.TextField(blank=True, null=True,
    help_text="Platform-neutral expression used to generate the surrogate key (e.g. hash256)."
  )
  profiling_stats = models.JSONField(blank=True, null=True, 
    help_text=(
      "Optional summarized profiling statistics for this column, stored as JSON. "
      "Use valid JSON syntax (key-value pairs). Example: "
      '{"null_rate": 0.02, "distinct_count": 123, "top_values": ["A", "B", "C"]}'
    )
  )
  active = models.BooleanField(default=True, 
    help_text=(
      "If unchecked, this column is deprecated. It remains in metadata for lineage "
      "and documentation but should not be generated, deployed, or used for new models."
    )
  )
  retired_at = models.DateTimeField(blank=True, null=True,
    help_text=(
      "Optional timestamp when this column was marked as inactive. "
      "Automatically set when active is unchecked."
    )
  )
  lineage_key = models.CharField(max_length=255, null=True, blank=True, db_index=True,
    help_text=(
      "Stable technical key used by the generator to identify this column "
      "by its source lineage instead of its physical name. "
      "Ensures that renaming target_column_name does not create duplicates."
    ),
  )
  former_names = models.JSONField(blank=True, default=list,
    help_text=(
      "List of previous physical column names for this TargetColumn. "
      "Used by materialization sync to detect renames and emit RENAME COLUMN "
      "instead of ADD COLUMN. Technical governance field; do not edit manually."
    ),
  )
  is_system_managed = models.BooleanField(default=False,
    help_text="If checked, this column is managed by the system and core attributes are locked."
  )

  class Meta:
    db_table = "target_column"
    constraints = [
      models.UniqueConstraint(
        fields=["target_dataset", "target_column_name"],
        name="unique_target_column"
      ),
      models.UniqueConstraint(
        fields=["target_dataset", "ordinal_position"],
        name="unique_target_column_position"
      ),
    ]
    ordering = ["target_dataset", "ordinal_position"]
    verbose_name_plural = "Target Columns"

  @property
  def is_protected_name(self) -> bool:
    return (self.system_role or "") in {
      "surrogate_key",
      "foreign_key",
      "entity_key",
      "row_hash",
      "load_run_id",
      "loaded_at",
      "version_started_at",
      "version_ended_at",
      "version_state",
    }

  def __str__(self):
    return display_key(self.target_dataset, self.target_column_name)

  def save(self, *args, **kwargs):
    # Only set an ordinal_position if neither pk exists nor a value is set.
    if not self.pk and not self.ordinal_position:
      max_ord = (
        TargetColumn.objects
        .filter(target_dataset=self.target_dataset)
        .aggregate(m=Max("ordinal_position"))
        .get("m") or 0
      )
      self.ordinal_position = max_ord + 1

    # handle retired_at
    if not self.active and self.retired_at is None:
      self.retired_at = timezone.now()
    elif self.active:
      self.retired_at = None

    super().save(*args, **kwargs)

  def clean(self):
    if self.system_role and not self.is_system_managed:
      raise ValidationError(
        "system_role can only be set on system-managed columns."
      )


# -------------------------------------------------------------------
# TargetColumnInput
# -------------------------------------------------------------------
class TargetColumnInput(AuditFields):
  target_column = models.ForeignKey(TargetColumn, on_delete=models.CASCADE, related_name="input_links",
    help_text="The target column being populated."
  )
  # Either a direct source column...
  source_column = models.ForeignKey(SourceColumn, on_delete=models.PROTECT, null=True, blank=True, related_name="output_links",
    help_text="The original source column feeding this target column."
  )
  # ...or an upstream target column (Raw -> Stage -> Rawcore).
  upstream_target_column = models.ForeignKey(TargetColumn, on_delete=models.PROTECT, null=True, blank=True, related_name="downstream_column_inputs",
    help_text="If set, this target column is fed from another target column instead of a raw source column."
  )
  manual_expression = models.TextField(blank=True, null=True,
    help_text=(
      "Optional expression that applies ONLY when using this specific source column.\n"
      "- Prefer elevata DSL wrapped in {{ ... }} for generation.\n"
      "- If you enter plain SQL without {{ }}, it may be used directly in SQL preview "
      "for this input.\n"
      "If empty, the TargetColumn.manual_expression (or a direct column reference) is used."
    ),
  )
  ordinal_position = models.PositiveIntegerField(default=1, 
    help_text=(
      "Priority / fallback order when multiple source systems "
      "feed the same target column. 1 = highest priority."
    )
  )
  active = models.BooleanField(default=True,
    help_text="If unchecked, kept for lineage/history but ignored going forward."
  )

  class Meta:
    db_table = "target_column_input"
    constraints = [
      models.UniqueConstraint(
        fields=["target_column", "source_column", "upstream_target_column"],
        name="unique_target_column_input_source"
      ),
      models.CheckConstraint(
        name="td_input_exactly_one_column_upstream",
        condition=(
          models.Q(source_column__isnull=False, upstream_target_column__isnull=True) |
          models.Q(source_column__isnull=True, upstream_target_column__isnull=False)
        ),
      )
    ]
    ordering = ["target_column", "ordinal_position"]

  def __str__(self):
    """
    Human-readable representation of this column-level lineage.

    - If we have a source_column, show that.
    - Otherwise, if we have an upstream_target_column, show that.
    - Otherwise, show a dash.
    """
    if getattr(self, "source_column_id", None):
      src_label = str(self.source_column)
    elif getattr(self, "upstream_target_column_id", None):
      src_label = str(self.upstream_target_column)
    else:
      src_label = "—"
    return f"{src_label} -> {self.target_column}"


# -------------------------------------------------------------------
# TargetDatasetReference
# -------------------------------------------------------------------
class TargetDatasetReference(AuditFields):
  referencing_dataset = models.ForeignKey(TargetDataset, on_delete=models.CASCADE, related_name="outgoing_references",
    help_text="Child dataset that holds the foreign key."
  )
  referenced_dataset = models.ForeignKey(TargetDataset, on_delete=models.CASCADE, related_name="incoming_references",
    help_text="Parent dataset that defines the business entity / surrogate PK."
  )
  reference_prefix = models.CharField(max_length=10, blank=True, null=True, validators=[SHORT_NAME_VALIDATOR],
    help_text=(
      "If provided, forms the FK name in the child as <prefix>_<referenced_dataset>_key. "
      "Example: prefix 'billing' + referenced 'sap_customer' -> 'billing_sap_customer_key'."
    )
  )
  relationship_type = models.CharField(max_length=20, choices=RELATIONSHIP_TYPE_CHOICES, default="n_to_1",
    help_text="Type of relationship of this reference."
  )
  join_condition_hint = models.CharField(max_length=255, blank=True, null=True,
    help_text=(
      "Human/machine-readable join hint for lineage visualization. "
      "Example: 'child.billing_sap_customer_key = parent.sap_customer_key'."
    )
  )

  class Meta:
    db_table = "target_dataset_reference"
    constraints = [
      models.UniqueConstraint(
        fields=["referencing_dataset", "reference_prefix", "referenced_dataset"],
        name="unique_target_dataset_reference"
      )
    ]
    ordering = ["referencing_dataset", "reference_prefix", "referenced_dataset"]
    verbose_name_plural = "Target Dataset References"

  def __str__(self):
    return f"{self.referencing_dataset} -> {self.referenced_dataset} ({self.reference_prefix or ''})"
  
  @property
  def missing_bk_components(self) -> list[str]:
    """
    Return parent BK column names that are not covered by any key component.

    This only checks the FK mapping coverage (components),
    not Stage lineage or expressions.
    """
    # All active BK columns on the parent dataset, in key order
    parent_bk_cols = list(
      self.referenced_dataset.target_columns
      .filter(system_role="business_key", active=True)
      .order_by("ordinal_position", "id")
      .values_list("target_column_name", flat=True)
    )

    # All parent BK columns that are actually mapped by components
    mapped_parent_cols = set(
      self.key_components
      .filter(to_column__isnull=False)
      .values_list("to_column__target_column_name", flat=True)
    )

    # Those parent BK columns that do not appear in the mapping
    return [name for name in parent_bk_cols if name not in mapped_parent_cols]

  @property
  def has_incomplete_bk_components(self) -> bool:
    """
    True if at least one parent BK column has no mapping component.
    """
    return bool(self.missing_bk_components)
  
  def validate_key_components(self) -> list[str]:
    """
    Return a list of missing parent BK column names.

    If empty, the mapping is complete.
    This method does NOT use an 'active' flag on components,
    because components are either present or deleted.
    """
    # All BK columns on the parent, in defined order
    parent_bk_names = list(
      self.referenced_dataset.target_columns
      .filter(system_role="business_key")
      .order_by("ordinal_position", "id")
      .values_list("target_column_name", flat=True)
    )

    # To-Columns that are mapped by components
    mapped_parent_bk_names = set(
      self.key_components
      .values_list("to_column__target_column_name", flat=True)
    )

    # Missing = parent BKs that are not mapped
    missing = [name for name in parent_bk_names if name not in mapped_parent_bk_names]
    return missing
  
  def get_child_fk_name(self) -> str:
    """
    Compute the FK column name on the child dataset.

    Convention:
      - Base name derived from the parent dataset (surrogate key name)
      - Optional prefix: <prefix>_<base_name>
    """
    # Parent dataset holds the surrogate key
    parent_ds = self.referenced_dataset

    # Base FK name like "<parent>_key"
    base_fk_name = naming.build_surrogate_key_name(
      parent_ds.target_dataset_name
    )

    # Optional prefix, e.g. "billing_sap_customer_key"
    if self.reference_prefix:
      return f"{self.reference_prefix}_{base_fk_name}"

    return base_fk_name

  def sync_child_fk_column(self):
    """
    Ensure the FK surrogate column exists and is up-to-date.

    Logic:
      1) Check if all required BK components exist
      2) If complete → build expression via builder (single source of truth)
      3) Create or update the FK column
      4) Mark as system-managed + foreign key column
    """

    reference = self
    child = reference.referencing_dataset

    # ----------------------------------------------------------------------
    # 1) Check BK completeness
    # ----------------------------------------------------------------------
    if self.has_incomplete_bk_components:
      return None

    # ----------------------------------------------------------------------
    # 2) Build final FK expression via central builder method
    # ----------------------------------------------------------------------
    fk_expression_sql = build_surrogate_fk_expression(reference)
    # builder returns either RawSql(...) or a ConcatExpression with correct .sql

    # ----------------------------------------------------------------------
    # 3) Determine FK column name
    # ----------------------------------------------------------------------
    fk_name = reference.get_child_fk_name()

    # Stable identity for the FK column, independent of renames.
    fk_lineage_key = f"fk:{reference.id}"

    # Prefer lookup by lineage_key to avoid ambiguity when a child has multiple references.
    fk_col = TargetColumn.objects.filter(
      target_dataset=child,
      lineage_key=fk_lineage_key,
    ).first()

    if fk_col is None:
      # Prefer adopting an existing column with the target FK name.
      # This avoids duplicates when legacy data/tests pre-create the FK column.
      by_name = list(TargetColumn.objects.filter(
        target_dataset=child,
        target_column_name=fk_name,
      )[:2])

      if len(by_name) == 1:
        fk_col = by_name[0]
        fk_col.lineage_key = fk_lineage_key

      elif len(by_name) > 1:
        logger.warning(
          "FK sync ambiguous: multiple columns with fk_name; ref_id=%s child=%s fk_name=%s",
          getattr(reference, "id", None),
          getattr(child, "target_dataset_name", None),
          fk_name,
        )
        return None

      else:
        # Best-effort adoption for existing FK columns created before lineage_key was introduced.
        expr = fk_expression_sql.sql if hasattr(fk_expression_sql, "sql") else str(fk_expression_sql)
        candidates = list(TargetColumn.objects.filter(
          target_dataset=child,
          is_system_managed=True,
          system_role="foreign_key",
          lineage_origin="foreign_key",
          surrogate_expression=expr,
        )[:2])

        if len(candidates) == 1:
          fk_col = candidates[0]
          fk_col.lineage_key = fk_lineage_key

        elif len(candidates) > 1:
          logger.warning(
            "FK sync ambiguous: multiple FK columns match surrogate_expression; ref_id=%s child=%s expected_fk_name=%s",
            getattr(reference, "id", None),
            getattr(child, "target_dataset_name", None),
            fk_name,
          )
          return None

        else:
          fk_col = TargetColumn(
            target_dataset=child,
            target_column_name=fk_name,
            lineage_key=fk_lineage_key,
          )

    # If the computed FK name changed due to parent/child rename, rename the column in metadata
    # and keep former_names so the planner can produce RENAME_COLUMN.
    if fk_col.target_column_name != fk_name:
      former = list(fk_col.former_names or [])
      if fk_col.target_column_name and fk_col.target_column_name not in former:
        former.append(fk_col.target_column_name)
      fk_col.former_names = former
      fk_col.target_column_name = fk_name

    # ----------------------------------------------------------------------
    # 4) Update existing FK column
    # ----------------------------------------------------------------------
    fk_col.datatype = "STRING"
    fk_col.max_length = 64
    fk_col.nullable = True
    fk_col.is_system_managed = True
    fk_col.system_role = "foreign_key"
    fk_col.lineage_origin = "foreign_key"
    fk_col.surrogate_expression = (
      fk_expression_sql.sql
      if hasattr(fk_expression_sql, "sql")
      else str(fk_expression_sql)
    )

    fk_col.save()

    return fk_col

  def save(self, *args, **kwargs):
    super().save(*args, **kwargs)
    # Try to sync FK column whenever the reference itself changes.
    # This will only create/update the FK if the BK components are complete.
    try:
      # Keep outer transactions healthy if FK sync hits an IntegrityError.
      with transaction.atomic():
        self.sync_child_fk_column()

    except Exception:
      # defensive: FK-Sync should not force errors in normal save
      pass


  def delete(self, *args, **kwargs):
    """
    When deleting a reference, also remove the system-managed FK column on the child.
    Additionally, remove dependent historization columns/inputs (e.g. *_hist) that may
    PROTECT the FK via upstream references.
    """
    try:
      child = getattr(self, "referencing_dataset", None)

      # Ultra-safe guard:
      # Only do hist/downstream cleanup for Rawcore datasets that are actually historized.
      do_hist_cleanup = False
      try:
        if child is not None and child.target_schema.short_name == "rawcore" and bool(child.historize):
          do_hist_cleanup = True
      except Exception:
        do_hist_cleanup = False

      fk_lineage_key = f"fk:{self.id}"
      fk_col = None
      if child is not None:
        fk_col = TargetColumn.objects.filter(
          target_dataset=child,
          lineage_key=fk_lineage_key,
        ).first()

      # Delete the reference row first (so UI state is consistent),
      # but keep cleanup in the same outer transaction if the caller uses one.
      super().delete(*args, **kwargs)

      if fk_col is None:
        return

      # If not historized rawcore, we still delete the FK column itself, but skip hist/downstream handling.
      # (Keeps behavior conservative outside historization.)

      # 1) Find downstream columns that depend on this FK as an upstream input
      # (most commonly: historization datasets like "<child>_hist").
      downstream_cols = list(
        TargetColumn.objects.filter(
          input_links__upstream_target_column=fk_col,
        ).select_related("target_dataset").distinct()
      )

      # 2) POLICY: Never drop columns in *_hist.
      # Detach inputs so FK is not protected anymore.
      # Additionally, if this is a historized rawcore dataset, HARD-deactivate downstream hist/FK columns.
      for c in downstream_cols:
        # Always detach the FK-upstream link on downstream columns
        try:
          TargetColumnInput.objects.filter(
            target_column=c,
            upstream_target_column=fk_col,
          ).delete()
        except Exception:
          pass

        if not do_hist_cleanup:
          continue

        ds = getattr(c, "target_dataset", None)
        role = (getattr(c, "system_role", "") or "").strip()
        is_hist_ds = ds.is_hist
        is_fk_role = (role == "foreign_key")

        if not (is_hist_ds or is_fk_role):
          continue

        # Hard deactivate (avoid any model save / signals reactivating)
        try:
          TargetColumn.objects.filter(pk=c.pk).update(
            active=False,
            retired_at=timezone.now(),
          )
        except Exception:
          pass

      # 3) As a last resort, delete any remaining inputs that reference the FK
      try:
        TargetColumnInput.objects.filter(upstream_target_column=fk_col).delete()
      except Exception:
        pass

      # 4) Now delete the FK column itself
      try:
        fk_col.delete()
      except Exception:
        pass

      # 5) Best-effort: re-sync hist metadata so orphan preservation can take effect
      try:
        if (
          child is not None
          and getattr(getattr(child, "target_schema", None), "short_name", None) == "rawcore"
          and bool(getattr(child, "historize", False))
        ):
          from metadata.generation.target_generation_service import TargetGenerationService
          svc = TargetGenerationService()
          svc.ensure_hist_dataset_for_rawcore(child)

      except Exception:
        pass

    except Exception:
      # Never block reference deletion due to cleanup issues
      try:
        super().delete(*args, **kwargs)
      except Exception:
        pass

# -------------------------------------------------------------------
# TargetDatasetReferenceComponent
# -------------------------------------------------------------------
class TargetDatasetReferenceComponent(AuditFields):
  reference = models.ForeignKey(TargetDatasetReference, on_delete=models.CASCADE, related_name="key_components",
    help_text="The parent-child relationship this mapping belongs to."
  )
  from_column = models.ForeignKey(TargetColumn, on_delete=models.PROTECT, related_name="fk_components_outgoing",
    help_text="Column on the referencing (child) dataset."
  )
  to_column = models.ForeignKey(TargetColumn, on_delete=models.PROTECT, related_name="fk_components_incoming",
    help_text="Column on the referenced (parent) dataset."
  )
  ordinal_position = models.PositiveIntegerField(default=1,
    help_text="Order for composite keys."
  )

  class Meta:
    db_table = "target_reference_key_component"
    constraints = [
      models.UniqueConstraint(
        fields=["reference", "from_column", "to_column"],
        name="unique_reference_parent_key_component"
      )
    ]
    ordering = ["reference", "ordinal_position"]
    verbose_name_plural = "Target Reference Components"

  def __str__(self):
    return f"{self.reference.referencing_dataset}.{self.from_column.target_column_name} -> {self.reference.referenced_dataset}.{self.to_column.target_column_name} (#{self.ordinal_position})"

  def save(self, *args, **kwargs):
    super().save(*args, **kwargs)
    # After each change to key components, re-evaluate FK completeness
    try:
      with transaction.atomic():
        self.reference.sync_child_fk_column()
    except Exception:
      pass

  def delete(self, *args, **kwargs):
    ref = self.reference
    super().delete(*args, **kwargs)
    # After deleting a component, FK might become incomplete – we currently
    # do not drop it, but we could extend sync_child_fk_column() if nötig.
    try:
      with transaction.atomic():
        ref.sync_child_fk_column()
    except Exception:
      pass
