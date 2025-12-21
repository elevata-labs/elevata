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

import os
import hashlib
from django.db.models import Max
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
  TARGET_COMBINATION_MODE_CHOICES, BIZ_ENTITY_ROLE_CHOICES, INCREMENTAL_STRATEGY_CHOICES)
from metadata.generation.validators import SHORT_NAME_VALIDATOR, TARGET_IDENTIFIER_VALIDATOR

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
  # Physical placement on the target platform
  database_name = models.CharField(max_length=100, validators=[TARGET_IDENTIFIER_VALIDATOR],
    help_text="Target database / catalog on the destination platform."
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
  consolidate_groups = models.BooleanField(default=False,
    help_text="If true, datasets from multiple source systems may be merged/grouped into one physical dataset in this schema."
  )
  # Whether end users can actively model new datasets in this layer
  is_user_visible = models.BooleanField(default=True, 
    help_text="If unchecked, this layer is internal/technical and hidden in normal UIs."
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
  # Which layer / schema this dataset belongs to
  target_schema = models.ForeignKey(TargetSchema, on_delete=models.PROTECT, related_name="target_datasets",
    help_text="Defines physical DB/schema, default materialization and governance expectations."
  )
  # Logical / business-facing name of the dataset in the target platform
  target_dataset_name = models.CharField(max_length=63, validators=[TARGET_IDENTIFIER_VALIDATOR],
    help_text="Final dataset (table/view) name, snake_case. eg. 'sap_customer', 'sap_sales_order'."
  )
  description = models.CharField(max_length=255, blank=True, null=True,
    help_text="Business description / semantic meaning of the dataset."
  )
  # Incremental / historization behavior
  handle_deletes = models.BooleanField(default=True, 
    help_text="Whether deletes in the source should be reflected in this dataset."
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
      "- snapshot: periodic snapshots by watermark/date"
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
  data_filter = models.CharField(max_length=255, blank=True, null=True,
    help_text="Optional row-level filter to restrict records."
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
    return self.target_schema.short_name == "rawcore" and self.target_dataset_name.endswith("_hist")

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

    return f"{src_label} -> {self.target_dataset}"

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
  target_column_name = models.CharField(max_length=63, validators=[TARGET_IDENTIFIER_VALIDATOR],
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
  artificial_column = models.BooleanField(default=False,
    help_text=(
      "If checked, this column does not directly exist in any single source, "
      "but is computed / harmonized / derived."
    )
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

    fk_col, created = TargetColumn.objects.get_or_create(
        target_dataset=child,
        target_column_name=fk_name,
        defaults={
            "datatype": "string",
            "max_length": 64,
            "nullable": True,
            "is_system_managed": True,
            "system_role": "foreign_key",
            "lineage_origin": "foreign_key",
            "surrogate_expression": fk_expression_sql.sql
                if hasattr(fk_expression_sql, "sql")
                else str(fk_expression_sql),
        },
    )

    # ----------------------------------------------------------------------
    # 4) Update existing FK column
    # ----------------------------------------------------------------------
    fk_col.datatype = "string"
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
      self.sync_child_fk_column()
    except Exception:
      # defensive: FK-Sync should not force errors in normal save
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
      self.reference.sync_child_fk_column()
    except Exception:
      pass

  def delete(self, *args, **kwargs):
    ref = self.reference
    super().delete(*args, **kwargs)
    # After deleting a component, FK might become incomplete – we currently
    # do not drop it, but we could extend sync_child_fk_column() if nötig.
    try:
      ref.sync_child_fk_column()
    except Exception:
      pass
